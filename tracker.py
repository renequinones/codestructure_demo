from typing import Any, Callable, cast, DefaultDict, Dict, List, MutableMapping, Mapping, Optional, Tuple, Union
import logging
import copy
import re
from ast import literal_eval as make_tuple

import rtlib
import loramsg
import dbpools
import datetime
from opera.validate import Vdict, v_time, Validator, Vlist, Vtuple
from opera.validate import v_float, v_uint2, v_bool, v_any, v_int, v_int8, v_int4, v_str

from .defs import TrackerException
from .parser import parse, TrackerMessage

logger = logging.getLogger("trackers")

PGWR_TASK_STOP_DELAY = 5


def ts2dt(ts: float) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(ts)


def ts2str(ts: float) -> str:
    return str(ts2dt(ts))


def utcdt_to_pgstr(dt: datetime.datetime) -> str:
    return dt.isoformat() + 'Z'


def utcts_to_pgstr(ts: int) -> str:
    return utcdt_to_pgstr(datetime.datetime.utcfromtimestamp(ts))


def format_ts(v: float) -> str:
    return str(datetime.datetime.utcfromtimestamp(v))


def gen_upinfo_for_owner(upinfo: List[dict]) -> List[dict]:
    return [{'routerid': str(rtlib.RouterId(i['routerid'])), 'rssi': i['rssi'], 'snr': i['snr']} for i in upinfo]


def format_upinfo(upinfo: List[dict]) -> str:
    return '</br>'.join(['%s:%r/%r' % (str(rtlib.RouterId(i['routerid'])), i['rssi'], i['snr']) for i in upinfo])


class Tracker:

    def __init__(self, manager: 'TrackerManager', deveui: str, did: int) -> None:
        self.manager = manager
        self.deveui = deveui
        self.did = did

    def __str__(self):
        return 'tracker:%s:%d' % (self.deveui, self.did)


TRACKER_TABLES_CREATE_STMT = '''
CREATE TABLE IF NOT EXISTS trackers_%d_tracker_status (
       deveui    int8         PRIMARY KEY
);        
CREATE TABLE IF NOT EXISTS trackers_%d_tracker_message (
       msgid     BIGSERIAL    PRIMARY KEY,
       deveui    int8         NOT NULL,
       pos       point        NOT NULL,
       acc       int          NOT NULL,
       sensors   jsonb        NOT NULL,
       error     text,
       upid      int8         NOT NULL,
       lora      jsonb        NOT NULL,
       ts                     TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc')
);        
CREATE INDEX IF NOT EXISTS trackers_%d_tracker_message_deveui_idx ON trackers_%d_tracker_message(deveui);
CREATE INDEX IF NOT EXISTS trackers_%d_tracker_message_ts_idx ON trackers_%d_tracker_message(ts);
CREATE INDEX IF NOT EXISTS trackers_%d_tracker_message_upid_idx ON trackers_%d_tracker_message(upid);
'''


def getTablesCreateStmt(ownerid: rtlib.OwnerId) -> str:
    id = ownerid.id
    return TRACKER_TABLES_CREATE_STMT % (id, id, id, id, id, id, id, id)


PARSER_NAME = 'system/trackers/any/v10/beapp'


class TrackerManager:
    def __init__(self, ownerid: rtlib.OwnerId) -> None:
        self.ownerid = ownerid
        self.did2tracker = {}  # type: MutableMapping[int,Tracker]
        self.trackers = []  # type: List[Tracker]
        empty_list_fn = list  # type: Callable[[],List[TrackerMessage]]
        self.pgwrmessagetask = rtlib.BgTask(self.pg_message_store_action, empty_list_fn, 'message_pgwr_task',
                                            PGWR_TASK_STOP_DELAY)
        empty_dict_fn = dict  # type: Callable[[],MutableMapping[int,Tracker]]
        self.pgwrstatustask = rtlib.BgTask(self.pg_status_store_action, empty_dict_fn, 'status_pgwr_task',
                                           PGWR_TASK_STOP_DELAY)

    async def __aioinit__(self) -> None:
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(getTablesCreateStmt(self.ownerid))
        self.did2tracker = await self.loadTracker()
        self.pgwrmessagetask.start()
        self.pgwrstatustask.start()

    async def shutdown(self) -> None:
        await self.pgwrmessagetask.stop()
        await self.pgwrstatustask.stop()

    def get_status_table_name(self) -> str:
        return 'trackers_%d_tracker_status' % (self.ownerid.id)

    def get_message_table_name(self) -> str:
        return 'trackers_%d_tracker_message' % (self.ownerid.id)

    async def loadTracker(self) -> MutableMapping[int, 'Tracker']:
        did2t = {}  # type: MutableMapping[int,Tracker]
        q = 'select deveui from %s' % (self.get_status_table_name())
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(q)
            rows = await cur.fetchall()
            for r in rows:
                did = r[0]
                t = Tracker(self, rtlib.Eui.int2str(did), did)
                did2t[did] = t
            return did2t

    async def getTracker(self, deveui: str) -> 'Tracker':
        did = rtlib.Eui.str2int(deveui)
        tracker = self.did2tracker.get(did)
        if tracker is not None:
            return tracker
        tracker = Tracker(self, deveui, did)
        self.pgwrstatustask.queue[did] = tracker
        self.pgwrstatustask.notify()
        self.did2tracker[did] = tracker
        return tracker

    async def addTracker(self, deveui: str, did: int) -> 'Tracker':
        assert self.did2tracker.get(did) is None
        tracker = Tracker(self, deveui, did)
        self.pgwrstatustask.queue[did] = tracker
        self.pgwrstatustask.notify()
        self.did2tracker[did] = tracker
        return tracker

    async def on_lora_msg(self, msg: loramsg.Msg) -> None:
        if msg['msgtype'] != 'upinfo':
            return
        deveui = msg['DevEui']
        did = rtlib.Eui.str2int(deveui)
        t = self.did2tracker.get(did, None)
        s = 'uncategorized device: %s' % (deveui) if t is None else 'tracker device: %s' % (deveui)
        try:
            tm = await parse(PARSER_NAME, did, msg)
            if tm is None:
                logger.info('%s: ignore unparsed message for %s.' % (self, s))
                return
        except Exception as ex:
            if t is None:
                # logger.info('%s: ignore parser error for: %s' % (self, s))
                logger.error('%s: ignore parser error for: %s' % (self, s), exc_info=True)
            else:
                logger.error('%s: ignore parser error for: %s' % (self, s), exc_info=True)
            return
        if t is None:
            t = await self.addTracker(deveui, did)
        self.pgwrmessagetask.queue.append(tm)
        self.pgwrmessagetask.notify()

    async def getTrackerMessages(self, dids: List[int], fro: datetime.datetime, to: datetime.datetime) -> Mapping[
        int, List[TrackerMessage]]:
        dids = [did for did in dids if did in self.did2tracker]
        if not dids:
            return {}
        q = "SELECT deveui,pos,acc,sensors,error,upid,lora,extract(epoch from ts) FROM %s WHERE " % (
            self.get_message_table_name())
        q += "deveui IN (%s) AND " % ','.join(map(str, dids))
        q += ("upid BETWEEN %s AND %s ORDER BY upid ASC LIMIT %d" %
              (rtlib.upid_from_datetime(fro), rtlib.upid_from_datetime(to), 8192))
        # print('query: %s' % q)
        did2track = {}
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(q)
            async for deveui, pos, acc, sensors, error, upid, lora, ts in cur:
                if deveui not in did2track:
                    did2track[deveui] = []
                pos = make_tuple(pos)
                tm = TrackerMessage(deveui, upid, lora, datetime.datetime.utcfromtimestamp(ts), pos, acc, error,
                                    sensors)
                did2track[deveui].append(tm)
        return did2track

    def getDid2Tracker(self) -> Mapping[int, 'Tracker']:
        return self.did2tracker

    def lookupTracker(self, deveui: str) -> Optional['Tracker']:
        did = rtlib.Eui.str2int(deveui)
        return self.did2tracker.get(did, None)

    def getTrackers(self, off: int, cnt: int) -> List[Tracker]:
        if len(self.trackers) != len(self.did2tracker):
            self.trackers = list(self.did2tracker.values())
        return self.trackers[off:off + cnt]

    def filterTrackers(self, dids: List[int]) -> List[Tracker]:
        return [self.did2tracker[did] for did in dids if did in self.did2tracker]

    async def getLatestUpid(self) -> Optional[int]:
        q = 'select upid from %s order by upid desc limit 1' % (self.get_message_table_name())
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(q)
            rows = await cur.fetchall()
            return rows[0][0] if rows else None

    def searchTracker(self, s: str) -> Optional['Tracker']:
        def tn2eui(x):
            return "-".join([x[2:][i:i + 2] for i in range(0, 16, 2)])

        def s2eui(x):
            return "-".join([x[0:][i:i + 2] for i in range(0, len(x), 2)])

        s = s.upper()
        s = re.sub(r'.*TN', 'TN', s)
        if len(s) == 18:  # "TN58A0CB00002007B9"
            s = tn2eui(s)
            return self.lookupTracker(s)
        elif len(s) == 0:
            s = tn2eui("TN58A0CB0000200000")
            return self.lookupTracker(s)
        elif len(s) < 18 and "-" not in s:
            s = s2eui(s[::-1])[::-1]
            for did, t in self.did2tracker.items():
                if t.deveui.endswith(s):
                    return t
        elif len(s) < 23 and "-" in s:
            s = s.replace("-", "")
            s = s2eui(s[::-1])[::-1]
            for did, t in self.did2tracker.items():
                if t.deveui.endswith(s):
                    return t
        try:
            did = rtlib.Eui.str2int(s)
            if did in self.did2tracker:
                return self.did2tracker[did]
        except:  # noqa
            pass
        return None

    async def pg_message_store_action(self, queue: List[TrackerMessage]) -> None:
        sa = [
            "(%d, point(%f, %f), %d, %s, %s, %d, %s, '%s')" %
            (d.did, d.pos[0], d.pos[1], d.acc, rtlib.pg_dumps(d.sensors),
             rtlib.pg_quote_string(d.error) if d.error else 'NULL',
             d.upid, rtlib.pg_dumps(d.lora), utcdt_to_pgstr(d.ts))
            for d in queue
        ]
        keys = ['deveui', 'pos', 'acc', 'sensors', 'error', 'upid', 'lora', 'ts']
        q = 'INSERT INTO %s(%s) VALUES %s' % (self.get_message_table_name(), ','.join(keys), ','.join(sa))
        # print('query: %s' % (q))
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(q)

    async def pg_status_store_action(self, queue: MutableMapping[int, Tracker]) -> None:
        dids = list(queue.keys())
        sa = []
        for did in dids:
            t = self.did2tracker[did]
            sa.append("(%d)" % (t.did))
        keys = ['deveui']
        update_set = ','.join(['%s=EXCLUDED.%s' % (k, k) for k in keys])
        q = ('INSERT INTO %s(%s) VALUES %s ON CONFLICT (deveui) DO UPDATE SET %s'
             % (self.get_status_table_name(), ','.join(keys), ','.join(sa), update_set))
        with await dbpools.cursor('resdb') as cur:
            await cur.execute(q)
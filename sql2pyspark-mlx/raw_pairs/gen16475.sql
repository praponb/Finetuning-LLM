with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 30 days then 1 else 0 end as session_start_flag
    from web_events
),
sessions_web_events as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from flagged
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.duration_sec,
    s.session_start_flag,
    (select max(t2.duration_sec) from web_events t2 where t2.session_id = s.session_id) as max_web_event_duration_sec
from sessions_web_events s
order by session_id desc;

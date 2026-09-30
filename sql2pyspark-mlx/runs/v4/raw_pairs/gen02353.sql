with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        dayofweek(started_at) as started_at_dayofweek,
        substring(browser, 1, 3) as browser_prefix
    from sessions
    where country in ('JP', 'US')
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.web_event_event_time,
        latest.duration_sec
    from base s
    left join lateral (
            select c.event_time as web_event_event_time, c.duration_sec
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) latest on true
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(web_event_event_time) over (partition by country order by web_event_event_time, id) is null
                 or web_event_event_time > lag(web_event_event_time) over (partition by country order by web_event_event_time, id) + interval 14 days then 1 else 0 end as session_start_flag
    from with_latest
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by country order by web_event_event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
prev_events as (
    -- Previous event time per key
    select *, lag(web_event_event_time) over (partition by device order by web_event_event_time, id) as prev_web_event_event_time
    from sessionized
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_web_event_event_time is null or web_event_event_time > prev_web_event_event_time + interval '30' day then 1 else 0 end as session_start_flag_2
    from prev_events
),
sessionized_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag_2) over (partition by device order by web_event_event_time, id rows between unbounded preceding and current row) as burst_id_2
    from flagged
)
select
    id,
    device,
    started_at,
    country,
    browser_prefix,
    web_event_event_time,
    duration_sec,
    burst_id,
    prev_web_event_event_time,
    case when device rlike '^(desktop|tablet)$' then 'CORE' else 'OTHER_ZONE' end as device_zone
from sessionized_sessions;

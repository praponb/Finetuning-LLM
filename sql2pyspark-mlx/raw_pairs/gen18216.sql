with with_prev as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from web_events
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 14 days then 1 else 0 end as is_new_session
    from with_prev
)
-- Running sum of the flag = session number
select
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    sum(is_new_session) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
from gaps;

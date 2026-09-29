with with_prev as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by session_id order by event_time, id) as prev_event_time
    from web_events
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 7 days then 1 else 0 end as new_group_flag
    from with_prev
)
-- Running sum of the flag = session number
select
    id,
    session_id,
    event_type,
    page,
    event_time,
    sum(new_group_flag) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
from gaps

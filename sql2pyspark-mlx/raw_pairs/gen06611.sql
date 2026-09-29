with valid as (
    -- Filter the raw rows and derive helper columns
    select *, to_date(event_time) as event_time_date
    from web_events
    where event_time > timestamp '2023-03-19 05:00:00'
      and page is not null
),
with_prev as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by session_id order by event_time, id) as prev_event_time
    from valid
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 7 days then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by session_id order by event_time, id rows between unbounded preceding and current row) as session_no
    from with_gaps
)
-- Grouped metrics
select
    date_trunc('month', prev_event_time) as activity_month,
    min(session_no) as min_session_no,
    round(avg(is_new_session), 2) as avg_is_new_session,
    bit_and(duration_sec) as duration_sec_bit_and
from islands
group by date_trunc('month', prev_event_time)
order by activity_month nulls first

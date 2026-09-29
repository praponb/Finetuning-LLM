with with_gaps as (
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval 60 days then 1 else 0 end as new_group_flag
    from web_events
),
islands as (
    select
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from with_gaps
),
latest as (
    select *
    from (
        select *, row_number() over (partition by page order by event_time desc, id) as row_num
        from islands
    ) x
    where row_num = 1
),
with_prev as (
    select *, lag(event_time) over (partition by page order by event_time, id) as prev_event_time
    from latest
),
flagged as (
    select *, case when prev_event_time is null or event_time > prev_event_time + interval '14' day then 1 else 0 end as is_new_session
    from with_prev
),
sessions_web_events as (
    select *, sum(is_new_session) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
flagged_2 as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 14 days then 1 else 0 end as session_start_flag
    from sessions_web_events
),
sessions_web_events_2 as (
    select
        id,
        page,
        event_time,
        duration_sec,
        session_id_2,
        row_num,
        prev_event_time,
        burst_id,
        sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as session_no
    from flagged_2
)
select
    id,
    page,
    duration_sec,
    session_no
from sessions_web_events_2
order by id desc nulls first

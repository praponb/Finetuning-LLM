with flagged as (
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval 60 days then 1 else 0 end as new_group_flag
    from web_events
),
islands as (
    select *, sum(new_group_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    id,
    session_id,
    event_type,
    page,
    event_time,
    new_group_flag,
    burst_id
from islands

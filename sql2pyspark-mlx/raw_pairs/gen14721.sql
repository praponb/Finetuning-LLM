with prev_events as (
    select *, lag(started_at) over (partition by device order by started_at, id) as prev_started_at
    from sessions
),
gaps as (
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '60' day then 1 else 0 end as session_start_flag
    from prev_events
),
islands as (
    select *, sum(session_start_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
select
    id,
    customer_id,
    device,
    started_at,
    prev_started_at,
    case when device rlike '^(desktop|mobile)$' then 'EXTENDED' else 'GROUP_A' end as device_zone
from islands
order by customer_id

with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        substring(device, 1, 4) as device_prefix
    from sessions
    where customer_id is not null
),
with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by device order by started_at, id) as prev_started_at
    from cleaned
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '30' day then 1 else 0 end as is_new_burst
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_no
    from flagged
)
-- Multi-level aggregation
select
    case when session_no >= 7 then 'LARGE' else 'SMALL' end as session_no_tier,
    count(distinct customer_id) as distinct_customer_ids,
    count(distinct device) as distinct_devices
from sessionized
group by case when session_no >= 7 then 'LARGE' else 'SMALL' end
order by distinct_devices desc nulls first;

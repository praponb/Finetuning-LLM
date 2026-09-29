with filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        hour(started_at) as started_at_hour
    from sessions
    where device = 'mobile'
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by country order by started_at, id) + interval '7' day then 1 else 0 end as is_new_burst
    from filtered
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    customer_id,
    device,
    burst_id,
    count(case when browser = 'Edge' then 1 end) as edge_count,
    max(started_at) as last_seen,
    sum(case when browser = 'Chrome' then is_new_burst else 0 end) as chrome_is_new_burst,
    grouping_id() as grouping_id
from sessionized
group by rollup (customer_id, device, burst_id)
having chrome_is_new_burst > 0
order by device nulls first

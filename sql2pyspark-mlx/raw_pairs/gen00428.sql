with filtered as (
    -- Keep recent, valid rows
    select
        *,
        hour(started_at) started_at_hour,
        lower(device) device_lower,
        coalesce(country, 'DIRECT') country_filled
    from sessions
    where started_at >= timestamp '2023-06-21 07:00:00'
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by country order by started_at asc, id asc) + interval 7 days then 1 else 0 end as is_new_burst
    from filtered
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        started_at_hour,
        device_lower,
        sum(is_new_burst) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
    from gaps
)
-- Aggregate with conditional metrics
select
    country,
    device,
    case when started_at_hour >= 87 then 'PREMIUM' else 'STANDARD' end as started_at_hour_tier,
    approx_percentile(burst_id, 0.5) as burst_id_p50,
    count(distinct device) as distinct_devices
from sessions_sessions
group by rollup (country, device, case when started_at_hour >= 87 then 'PREMIUM' else 'STANDARD' end)
having count(distinct device) >= 1
order by device desc nulls first;

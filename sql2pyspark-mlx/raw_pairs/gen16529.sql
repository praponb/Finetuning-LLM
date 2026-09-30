with valid as (
    select *, length(device) as device_length
    from sessions
    where started_at > to_timestamp('2023-03-13 22:00:00')
      and device <> 'tablet'
),
metrics as (
    select
        *,
        max(device_length) over (partition by customer_id order by device_length, id rows between unbounded preceding and current row) as running_max_device_length,
        avg(device_length) over (partition by customer_id order by device_length, id rows between unbounded preceding and current row) as running_avg_device_length,
        count(device_length) over (partition by customer_id order by device_length, id rows between unbounded preceding and current row) as running_count_device_length,
        min(device_length) over (partition by customer_id order by device_length, id rows between unbounded preceding and current row) as running_min_device_length
    from valid
    where browser in ('Firefox', 'Safari', 'Chrome')
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by device order by running_max_device_length desc, id) as rn
        from metrics
    ) t
    where rn = 1
),
scored as (
    select
        *,
        count(running_count_device_length) over w_ordered as running_count_device_length_count_7d,
        last_value(browser, true) over (order by started_at, id rows between unbounded preceding and current row) as last_known_browser,
        ntile(5) over (partition by device order by running_max_device_length, id) as running_max_device_length_bucket
    from deduped
    window w_ordered as (partition by customer_id order by cast(started_at as bigint) range between 604800 preceding and current row)
),
enriched as (
    select
        *,
        lead(running_max_device_length, 2) over (order by running_max_device_length_bucket, id) as next_running_max_device_length,
        lag(running_count_device_length_count_7d, 1) over (partition by country order by running_min_device_length, id) as prev_running_count_device_length_count_7d
    from scored
),
metrics_sessions as (
    select
        *,
        count(running_max_device_length_bucket) over (partition by browser, customer_id order by cast(started_at as bigint) range between 7776000 preceding and current row) as running_max_device_length_bucket_count_90d,
        last_value(running_max_device_length) ignore nulls over (partition by browser order by rn, id rows between unbounded preceding and current row) as last_known_running_max_device_length
    from enriched
)
select
    id,
    device,
    browser,
    started_at,
    country,
    device_length,
    running_max_device_length,
    running_count_device_length,
    running_min_device_length,
    case when browser rlike '^(Chrome|Edge)$' then 'PRIORITY' else 'EXTENDED' end as browser_zone
from metrics_sessions
order by device nulls first;

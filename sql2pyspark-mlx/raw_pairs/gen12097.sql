with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        k,
        value
    from carts
    lateral view outer explode(attributes) li as k, value
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when k = 'device' then 1 else 0 end as is_device,
        datediff(current_date(), created_at) as days_since_created_at
    from exploded
    where created_at > date '2025-03-02'
      and k is not null
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        created_at,
        k,
        is_device,
        days_since_created_at,
        coalesce(k, 'Unknown') as k_filled,
        coalesce(k, 'N/A') as k_filled_2
    from valid
    where customer_id is not null
      and k is not distinct from 'gift'
),
scored as (
    -- Ranking and gap features
    select
        *,
        count(is_device) over (partition by k order by cast(created_at as bigint) range between 3600 preceding and current row) as is_device_count_1h,
        avg(days_since_created_at) over (partition by k order by cast(created_at as bigint) range between 900 preceding and current row) as days_since_created_at_avg_15m,
        percent_rank() over (partition by customer_id, k order by days_since_created_at desc) as days_since_created_at_percent_rank
    from valid_carts
    where created_at >= '2024-08-01'
),
base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        created_at,
        is_device,
        days_since_created_at,
        k_filled,
        k_filled_2,
        is_device_count_1h,
        days_since_created_at_percent_rank,
        case when k in ('device', 'gift') then 'Core' else 'Other' end as k_group,
        coalesce(k, 'NONE') as k_filled_3
    from scored
    where created_at > date '2025-03-02'
      and k is not null
),
valid_carts_2 as (
    -- Basic cleansing and defaults
    select *, least(days_since_created_at_percent_rank, days_since_created_at) as least_days_since_created_at_percent_rank_days_since_created_at
    from base
    where customer_id is not null
),
enriched as (
    -- Ranking and gap features
    select
        customer_id,
        k_filled_2,
        days_since_created_at_percent_rank,
        k_group,
        cume_dist() over w_hist as is_device_count_1h_cume_dist,
        max(is_device) over (partition by customer_id order by cast(created_at as bigint) range between 1800 preceding and current row) as is_device_max_30m
    from valid_carts_2
    window w_hist as (partition by customer_id order by is_device_count_1h desc)
)
-- Multi-level aggregation
select
    case when days_since_created_at_percent_rank >= 0.85 then 'LARGE' else 'SMALL' end as days_since_created_at_percent_rank_tier,
    case when is_device_max_30m >= 82 then 'PREMIUM' else 'STANDARD' end as is_device_max_30m_tier,
    customer_id,
    count(*) as cnt,
    percentile_approx(days_since_created_at_percent_rank, array(0.25, 0.5, 0.75)) as days_since_created_at_percent_rank_quartiles,
    percentile_approx(is_device_max_30m, 0.9) as is_device_max_30m_p90,
    grouping(case when days_since_created_at_percent_rank >= 0.85 then 'LARGE' else 'SMALL' end) as grouping_id
from enriched
group by grouping sets ((case when days_since_created_at_percent_rank >= 0.85 then 'LARGE' else 'SMALL' end, case when is_device_max_30m >= 82 then 'PREMIUM' else 'STANDARD' end, customer_id), (case when days_since_created_at_percent_rank >= 0.85 then 'LARGE' else 'SMALL' end, case when is_device_max_30m >= 82 then 'PREMIUM' else 'STANDARD' end), (case when days_since_created_at_percent_rank >= 0.85 then 'LARGE' else 'SMALL' end))
order by is_device_max_30m_tier

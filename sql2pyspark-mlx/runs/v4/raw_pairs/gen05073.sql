with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) e as tag
),
cleaned as (
    -- Keep recent, valid rows
    select *, trim(tag) as clean_tag
    from flattened
    where tag in ('VIP', 'BUNDLE')
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        month(created_at) as created_at_month,
        datediff(current_date(), created_at) as days_since_created_at
    from cleaned
    where tag is not null
      and tag in ('SPRING10', 'VIP', 'WELCOME5')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        created_at,
        tag,
        clean_tag,
        created_at_month,
        days_since_created_at,
        cume_dist() over (partition by tag order by created_at_month) as created_at_month_cume_dist,
        max(created_at_month) over (partition by tag order by cast(created_at as bigint) range between 3600 preceding and current row) as created_at_month_max_1h,
        stddev_samp(created_at_month) over (partition by tag) as tag_stddev_samp_created_at_month
    from filtered
),
base as (
    -- Keep recent, valid rows
    select
        created_at,
        clean_tag,
        created_at_month,
        days_since_created_at,
        created_at_month_cume_dist,
        created_at_month_max_1h,
        tag_stddev_samp_created_at_month,
        coalesce(created_at_month_max_1h, 0) as created_at_month_max_1h_filled
    from with_windows
    where tag is not null
),
filtered_carts as (
    -- Keep recent, valid rows
    select
        *,
        case when created_at_month_cume_dist >= 0.84 then 'High' when created_at_month_cume_dist >= 0.12 then 'Medium' else 'Low' end as created_at_month_cume_dist_level,
        round(created_at_month_cume_dist * 0.85, 2) as created_at_month_cume_dist_scaled
    from base
    where created_at > date '2025-03-02'
)
-- Aggregate with conditional metrics
select
    date_trunc('hour', created_at) as hour_window,
    max(created_at) as last_created_at,
    round(avg(created_at_month), 2) as avg_created_at_month,
    min(created_at) as first_seen,
    approx_percentile(created_at_month_cume_dist_scaled, 0.9) as created_at_month_cume_dist_scaled_p90,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min,
    max(created_at) as ended_at
from filtered_carts
group by rollup (date_trunc('hour', created_at))
order by created_at_month_cume_dist_scaled_p90 desc

with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode(coupon_codes) as code
    from carts
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), created_at) / 7 as weeks_since_created_at,
        date_format(created_at, 'yyyy-MM') as created_at_label,
        coalesce(code, 'N/A') as resolved_code
    from exploded
    where customer_id in (11, 25, 36, 39)
      and customer_id is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select *, max(weeks_since_created_at) over (partition by date_trunc('WEEK', created_at), customer_id) as created_at_period_max_weeks_since_created_at
    from cleaned
),
with_windows_carts as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        created_at_label,
        created_at_period_max_weeks_since_created_at,
        count(weeks_since_created_at) over (partition by customer_id order by cast(created_at as bigint) range between 900 preceding and current row) as weeks_since_created_at_count_15m,
        cume_dist() over (partition by code order by weeks_since_created_at desc nulls last) as weeks_since_created_at_cume_dist
    from with_windows
),
scored as (
    -- Window metrics over several different partitions
    select
        created_at_label,
        weeks_since_created_at_cume_dist,
        cume_dist() over w_ordered as created_at_period_max_weeks_since_created_at_cume_dist,
        stddev_samp(weeks_since_created_at_count_15m) over w_recent as customer_id_stddev_samp_weeks_since_created_at_count_15m,
        percent_rank() over w_recent_2 as created_at_period_max_weeks_since_created_at_percent_rank
    from with_windows_carts
    window w_ordered as (partition by customer_id order by created_at_period_max_weeks_since_created_at desc nulls first), w_recent as (partition by customer_id), w_recent_2 as (partition by customer_id order by created_at_period_max_weeks_since_created_at)
)
-- Aggregate with conditional metrics
select
    case when created_at_period_max_weeks_since_created_at_cume_dist >= 0.7 then 'LARGE' else 'SMALL' end as created_at_period_max_weeks_since_created_at_cume_dist_tier,
    min(customer_id_stddev_samp_weeks_since_created_at_count_15m) as min_customer_id_stddev_samp_weeks_since_created_at_count_15m,
    min(created_at_period_max_weeks_since_created_at_percent_rank) as min_created_at_period_max_weeks_since_created_at_percent_rank,
    round(avg(weeks_since_created_at_cume_dist), 2) as avg_weeks_since_created_at_cume_dist,
    min(created_at_period_max_weeks_since_created_at_percent_rank) as min_created_at_period_max_weeks_since_created_at_percent_rank_2,
    round(avg(created_at_period_max_weeks_since_created_at_cume_dist), 2) as avg_created_at_period_max_weeks_since_created_at_cume_dist,
    approx_percentile(created_at_period_max_weeks_since_created_at_cume_dist, 0.5) as created_at_period_max_weeks_since_created_at_cume_dist_p50
from scored
group by 1
order by avg_created_at_period_max_weeks_since_created_at_cume_dist desc nulls last

with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        val
    from price_tiers
    lateral view outer explode(tier_prices) e as val
),
filtered as (
    -- Keep recent, valid rows
    select *, coalesce(val, 0) val_filled
    from lines
    where val between 33 and 39
),
filtered_price_tiers as (
    -- Basic cleansing and defaults
    select *, cast(val as int) val_int
    from filtered
    where val_filled > 46
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        max(val_filled) over w_hist plan_max_val_filled,
        sum(val_filled) over (partition by currency) currency_sum_val_filled,
        rank() over w_running val_rank,
        percent_rank() over (partition by currency order by val_int asc nulls last) val_int_percent_rank
    from filtered_price_tiers
    window w_hist as (partition by plan, currency), w_running as (partition by currency order by val desc nulls first)
),
daily as (
    -- Aggregate with conditional metrics
    select
        plan,
        min(val_rank) as min_val_rank,
        count(*) as row_count,
        percentile_approx(val_rank, 0.5) as val_rank_p50,
        count(distinct currency) as distinct_currencies,
        approx_percentile(val, 0.5) as val_p50
    from metrics
    group by plan
)
select
    plan,
    row_count,
    distinct_currencies,
    val_p50,
    case when plan rlike '^(basic|enterprise)$' then 'OTHER_ZONE' when plan rlike '^(starter|pro)$' then 'EXTENDED' else 'PRIORITY' end as plan_zone
from daily;

with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(discounts) as val
    from price_tiers
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, last_day(effective_from) as effective_from_month_end
    from lines
    where year(effective_from) >= 2025
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        date_add(effective_from_month_end, 65) as effective_from_month_end_plus_65d,
        case when plan = 'enterprise' then 1 else 0 end as is_enterprise
    from cleaned
    where effective_from >= '2023-06-18'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        plan,
        effective_from,
        val,
        effective_from_month_end_plus_65d,
        is_enterprise,
        dense_rank() over (partition by plan order by val desc nulls last) as val_dense_rank,
        percent_rank() over (partition by plan order by val desc) as val_percent_rank
    from base
    where effective_from_month_end_plus_65d > '2023-09-22'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        sum(val) over (partition by plan) as plan_sum_val,
        sum(val) over (partition by plan) as plan_sum_val_2
    from with_windows
)
select
    plan,
    val,
    effective_from_month_end_plus_65d,
    is_enterprise,
    val_percent_rank,
    plan_sum_val,
    plan_sum_val_2
from metrics;

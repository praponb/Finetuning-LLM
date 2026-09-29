with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode(discounts) as val
    from price_tiers
),
base as (
    -- Basic cleansing and defaults
    select *, round(val * 100, 2) as adjusted_val
    from exploded
    where plan in ('enterprise', 'starter', 'pro')
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        adjusted_val * val as adjusted_val_times_val,
        upper(plan) as plan_upper
    from base
    where plan in ('enterprise', 'pro')
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect all
    select id
    from price_tiers
    where year(effective_from) = 2025
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join remaining_keys k on s.id = k.id
)
select
    id,
    val,
    adjusted_val,
    adjusted_val_times_val,
    plan_upper
from reconciled;

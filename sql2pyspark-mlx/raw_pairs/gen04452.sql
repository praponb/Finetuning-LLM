with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(discounts) as tier_value
    from price_tiers
),
kept_keys as (
    -- Set intersection on keys
    select plan, id
    from flattened
    intersect all
    select plan, id
    from price_tiers
    where effective_from between '2023-05-01' and '2023-07-23'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened s
    inner join kept_keys k on s.plan = k.plan and s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select *, case when tier_value >= 65 then 'High' when tier_value >= 28 then 'Medium' else 'Low' end as tier_value_tier
    from reconciled
    where effective_from > '2025-06-15'
),
kept_keys_2 as (
    -- Set difference on keys
    select id
    from base
    except
    select id
    from price_tiers
    where effective_from between '2024-03-10' and '2024-06-01'
),
reconciled_price_tiers as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys_2 k on s.id = k.id
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        tier_value_tier,
        round(tier_value, 1) as tier_value_rounded,
        date_trunc('YEAR', effective_from) as effective_from_period
    from reconciled_price_tiers
    where year(effective_from) >= 2025
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, date_format(effective_from_period, 'yyyy-MM-dd') as effective_from_period_label
    from filtered
    where plan <> 'enterprise'
)
-- Multi-level aggregation
select
    plan,
    date_trunc('month', effective_from_period) as activity_month,
    count_if(plan = 'starter') as starter_events,
    count(*) filter (where plan = 'basic') as basic_rows,
    count(distinct plan) as distinct_plans,
    (unix_timestamp(max(effective_from_period)) - unix_timestamp(min(effective_from_period))) / 60 as span_minutes,
    max(tier_value_rounded) filter (where plan = 'basic') as basic_max_tier_value_rounded,
    percentile_approx(tier_value_rounded, 0.95) as tier_value_rounded_p95,
    grouping_id() as gid
from cleaned
group by rollup (plan, date_trunc('month', effective_from_period));

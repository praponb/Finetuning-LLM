with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        tier_value,
        avg(tier_value) over (partition by plan) as plan_avg_tier_value
    from lines
    where plan = 'pro'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(plan_avg_tier_value) over (partition by plan) as plan_avg_plan_avg_tier_value,
        max(tier_value) over (partition by plan) as plan_max_tier_value
    from with_windows
),
cleaned as (
    -- Basic cleansing and defaults
    select
        tier_value,
        plan_avg_tier_value,
        plan_avg_plan_avg_tier_value,
        plan_max_tier_value,
        least(tier_value, plan_avg_tier_value) as least_tier_value_plan_avg_tier_value
    from enriched
    where plan_max_tier_value = 27
),
with_windows_price_tiers as (
    -- Running and moving aggregates
    select
        tier_value,
        plan_avg_plan_avg_tier_value,
        plan_max_tier_value,
        least_tier_value_plan_avg_tier_value,
        rank() over (order by tier_value desc nulls first) as tier_value_rank
    from cleaned
),
metrics as (
    -- Running and moving aggregates
    select
        tier_value,
        tier_value_rank,
        dense_rank() over (order by plan_avg_plan_avg_tier_value desc nulls last) as plan_avg_plan_avg_tier_value_dense_rank,
        percent_rank() over w_part as plan_max_tier_value_percent_rank
    from with_windows_price_tiers
    window w_part as (order by plan_max_tier_value nulls first)
),
cleaned_price_tiers as (
    -- Filter the raw rows and derive helper columns
    select *, greatest(plan_avg_plan_avg_tier_value_dense_rank, tier_value_rank) as greatest_plan_avg_plan_avg_tier_value_dense_rank_tier_value_rank
    from metrics
    where plan_max_tier_value_percent_rank between 0.25 and 0.47
)
select
    tier_value,
    plan_avg_plan_avg_tier_value_dense_rank,
    plan_max_tier_value_percent_rank
from cleaned_price_tiers
order by plan_max_tier_value_percent_rank, tier_value desc

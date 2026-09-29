with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        explode(tier_prices) as tier_value
    from price_tiers
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by plan order by tier_value) as tier_value_dense_rank,
        rank() over (partition by date_trunc('YEAR', effective_from) order by tier_value desc nulls first) as tier_value_rank,
        cume_dist() over w_part as tier_value_cume_dist,
        avg(tier_value) over (partition by plan) as plan_avg_tier_value
    from flattened
    window w_part as (partition by date_trunc('week', effective_from), plan order by tier_value desc nulls last)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        tier_value,
        tier_value_dense_rank,
        tier_value_rank,
        tier_value_cume_dist,
        plan_avg_tier_value,
        datediff(current_date(), effective_from) / 365.25 as years_since_effective_from,
        tier_value / nullif(tier_value_cume_dist, 0) as tier_value_per_tier_value_cume_dist
    from metrics
    where effective_from >= date '2024-02-12'
      and plan = 'pro'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        tier_value,
        plan_avg_tier_value,
        years_since_effective_from,
        tier_value_per_tier_value_cume_dist,
        round(tier_value_per_tier_value_cume_dist, 1) as tier_value_per_tier_value_cume_dist_rounded
    from base
    where years_since_effective_from > 38
),
metrics_price_tiers as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over w_grp as tier_value_per_tier_value_cume_dist_percent_rank,
        rank() over w_grp as tier_value_per_tier_value_cume_dist_rank
    from valid
    where tier_value_per_tier_value_cume_dist is not null
    window w_grp as (order by tier_value_per_tier_value_cume_dist nulls first)
)
-- Aggregate with conditional metrics
select
    case when tier_value_per_tier_value_cume_dist_rounded >= 76 then 'HIGH' else 'LOW' end as tier_value_per_tier_value_cume_dist_rounded_tier,
    count(*) as num_rows,
    round(avg(tier_value_per_tier_value_cume_dist), 2) as avg_tier_value_per_tier_value_cume_dist,
    round(avg(tier_value_per_tier_value_cume_dist_rounded), 2) as avg_tier_value_per_tier_value_cume_dist_rounded
from metrics_price_tiers
group by cube (case when tier_value_per_tier_value_cume_dist_rounded >= 76 then 'HIGH' else 'LOW' end)
having avg_tier_value_per_tier_value_cume_dist > 0
order by avg_tier_value_per_tier_value_cume_dist_rounded;

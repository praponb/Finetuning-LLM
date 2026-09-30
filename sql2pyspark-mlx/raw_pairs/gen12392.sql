with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        val
    from price_tiers
    lateral view outer explode(discounts) e as val
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        lower(plan) plan_lower,
        coalesce(val, 0) val_filled
    from lines
    where effective_from > date '2024-06-15'
),
enriched as (
    -- Running and moving aggregates
    select
        plan,
        val,
        plan_lower,
        val_filled,
        cume_dist() over (partition by plan order by val desc) val_cume_dist
    from filtered
    where month(effective_from) in (4, 5, 9)
),
cleaned as (
    -- Keep recent, valid rows
    select
        val,
        plan_lower,
        val_filled,
        round(val, 0) val_rounded,
        val_cume_dist / nullif(val, 0) val_cume_dist_per_val
    from enriched
    where val <= 47
)
-- Multi-level aggregation
select
    case when val_filled >= 67 then 'HIGH' else 'LOW' end as val_filled_tier,
    case when val >= 63 then 'PREMIUM' else 'STANDARD' end as val_tier,
    avg(val_filled) as avg_val_filled,
    percentile_approx(val_filled, 0.95) as val_filled_p95,
    rank() over (partition by case when val_filled >= 67 then 'HIGH' else 'LOW' end order by percentile_approx(val_filled, 0.95) desc) as val_filled_p95_rank
from cleaned
group by case when val_filled >= 67 then 'HIGH' else 'LOW' end, case when val >= 63 then 'PREMIUM' else 'STANDARD' end
order by avg_val_filled

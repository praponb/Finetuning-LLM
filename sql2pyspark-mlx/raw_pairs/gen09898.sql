with unpacked as (
    -- Semi-structured parsing
    select
        id,
        plan,
        discounts,
        element_at(tier_prices, 1) as entry_price,
        array_min(tier_prices) as lowest_price,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
    where plan not in ('pro', 'enterprise')
)
-- Multi-level aggregation
select
    plan,
    count(*) filter (where plan = 'basic') as basic_rows,
    count_if(plan = 'starter') as starter_events,
    count(distinct plan) as distinct_plans
from unpacked
group by 1

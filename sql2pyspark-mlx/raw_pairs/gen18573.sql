with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        effective_from,
        tier_limits,
        tier_prices,
        aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
    from price_tiers
)
-- Window metrics over several different partitions
select
    id,
    effective_from,
    avg_tier_price,
    lead(avg_tier_price, 1) over (order by avg_tier_price asc, id asc) next_avg_tier_price,
    sum(avg_tier_price) over (order by avg_tier_price asc, id asc rows between unbounded preceding and current row) cumulative_avg_tier_price,
    sum(avg_tier_price) over (order by effective_from asc, id asc rows between unbounded preceding and current row) cumulative_avg_tier_price_2
from unpacked

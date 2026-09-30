with flattened_attrs as (
    -- Higher-order array functions
    select
        id,
        effective_from,
        transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
        element_at(tier_prices, 1) as entry_price,
        aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
        slice(tier_limits, 1, 2) as first_two_limits
    from price_tiers
)
-- Aggregate with conditional metrics
select
    date_trunc('month', effective_from) as activity_month,
    approx_percentile(entry_price, array(0.25, 0.5, 0.75)) as entry_price_quartiles,
    count(distinct id) as distinct_ids
from flattened_attrs
group by date_trunc('month', effective_from)
having distinct_ids < 1000
order by activity_month desc nulls first;

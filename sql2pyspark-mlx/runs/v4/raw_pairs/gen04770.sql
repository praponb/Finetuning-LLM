select
    id,
    effective_from,
    discounts,
    element_at(tier_prices, 1) as entry_price,
    array_min(tier_prices) as lowest_price,
    aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
    slice(tier_limits, 1, 2) as first_two_limits,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
from price_tiers
where effective_from >= '2024-02-01';

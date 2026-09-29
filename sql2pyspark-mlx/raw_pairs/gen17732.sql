select
    id,
    plan,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
    array_min(tier_prices) as lowest_price,
    element_at(tier_prices, 1) as entry_price,
    slice(tier_limits, 1, 2) as first_two_limits,
    aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price
from price_tiers

select
    id,
    plan,
    tier_limits,
    array_min(tier_prices) as lowest_price,
    element_at(tier_prices, 1) as entry_price,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
    slice(tier_limits, 1, 2) as first_two_limits,
    aggregate(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price
from price_tiers

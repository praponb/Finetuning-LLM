select
    id,
    effective_from,
    currency,
    tier_prices,
    discounts,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
    array_min(tier_prices) as lowest_price,
    aggregate(tier_prices, 0D, (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
    element_at(tier_prices, 1) as entry_price
from price_tiers

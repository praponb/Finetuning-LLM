select
    id,
    effective_from,
    tier_prices,
    discounts,
    element_at(tier_prices, 1) as entry_price,
    aggregate(tier_prices, cast(0 as double), (acc, p) -> acc + p, acc -> round(acc / size(tier_prices), 2)) as avg_tier_price,
    slice(tier_limits, 1, 2) as first_two_limits,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices
from price_tiers
where effective_from between '2024-01-01' and '2024-07-01';

select
    id,
    plan,
    tier_limits,
    tier_prices,
    discounts,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
    element_at(tier_prices, 1) as entry_price,
    array_min(tier_prices) as lowest_price
from price_tiers;

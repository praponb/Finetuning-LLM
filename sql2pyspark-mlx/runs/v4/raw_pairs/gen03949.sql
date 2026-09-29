select
    id,
    plan,
    effective_from,
    tier_limits,
    tier_prices,
    discounts,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps,
    slice(tier_limits, 1, 2) as first_two_limits
from price_tiers

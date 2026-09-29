select
    id,
    plan,
    zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices,
    slice(tier_limits, 1, 2) as first_two_limits,
    array_min(tier_prices) as lowest_price,
    transform(tier_prices, (p, i) -> round(p * tier_limits[i], 2)) as tier_revenue_caps
from price_tiers
where plan = 'basic';

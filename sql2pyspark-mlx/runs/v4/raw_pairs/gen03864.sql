select id, plan, discounts, array_min(tier_prices) as lowest_price, zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices from price_tiers

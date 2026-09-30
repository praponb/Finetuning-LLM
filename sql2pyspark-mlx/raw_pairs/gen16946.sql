select id, plan, effective_from, tier_prices, zip_with(tier_prices, discounts, (p, d) -> round(p * (1 - d), 2)) as net_prices, slice(tier_limits, 1, 2) as first_two_limits from price_tiers;

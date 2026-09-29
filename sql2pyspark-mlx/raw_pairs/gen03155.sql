SELECT id, plan, currency, SLICE(tier_limits, 1, 2) AS first_two_limits, ELEMENT_AT(tier_prices, 1) AS entry_price FROM price_tiers

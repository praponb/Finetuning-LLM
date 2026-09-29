SELECT id, plan, currency, EXPLODE_OUTER(tier_prices) AS element FROM price_tiers

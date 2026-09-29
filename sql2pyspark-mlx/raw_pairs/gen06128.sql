SELECT id, currency, EXPLODE(tier_limits) AS val FROM price_tiers

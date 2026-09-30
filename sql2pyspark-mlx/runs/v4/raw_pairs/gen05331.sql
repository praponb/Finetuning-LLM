SELECT id, plan, effective_from, currency, EXPLODE_OUTER(tier_prices) AS element FROM price_tiers;

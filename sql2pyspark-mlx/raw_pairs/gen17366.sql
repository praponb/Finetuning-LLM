SELECT id, plan, effective_from, EXPLODE_OUTER(tier_limits) AS tier_value FROM price_tiers;

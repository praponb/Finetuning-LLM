WITH flattened AS (SELECT id, plan, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) e AS tier_value) SELECT id, plan, tier_value FROM flattened ORDER BY tier_value DESC

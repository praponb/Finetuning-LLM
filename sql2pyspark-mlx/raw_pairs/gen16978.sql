WITH exploded AS (SELECT id, effective_from, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_prices) li AS tier_value) SELECT tier_value FROM exploded ORDER BY tier_value NULLS FIRST;

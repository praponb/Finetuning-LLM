WITH flattened AS (SELECT id, plan, EXPLODE_OUTER(tier_prices) AS tier_value FROM price_tiers) SELECT id, tier_value FROM flattened ORDER BY id DESC NULLS LAST;

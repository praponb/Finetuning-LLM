WITH exploded AS (SELECT id, EXPLODE_OUTER(tier_prices) AS element FROM price_tiers) SELECT element FROM exploded ORDER BY element DESC NULLS LAST

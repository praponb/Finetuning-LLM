WITH exploded AS (SELECT id, plan, effective_from, currency, EXPLODE(tier_prices) AS tier_value FROM price_tiers) SELECT id, effective_from, currency FROM exploded ORDER BY effective_from NULLS FIRST

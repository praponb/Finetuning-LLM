WITH exploded AS (SELECT id, plan, effective_from, EXPLODE(tier_prices) AS val FROM price_tiers) SELECT id, effective_from FROM exploded ORDER BY id

WITH exploded AS (SELECT id, EXPLODE(tier_prices) AS element FROM price_tiers) SELECT id, element FROM exploded ORDER BY id DESC

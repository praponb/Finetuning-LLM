WITH exploded AS (SELECT id, plan, currency, EXPLODE(tier_prices) AS element FROM price_tiers) SELECT id, plan FROM exploded ORDER BY plan ASC;

WITH lines AS (SELECT id, EXPLODE(tier_prices) AS tier_value FROM price_tiers) SELECT id, tier_value FROM lines ORDER BY id DESC NULLS FIRST

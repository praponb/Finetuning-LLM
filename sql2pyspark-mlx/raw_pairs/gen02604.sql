WITH lines AS (SELECT id, effective_from, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value) SELECT id FROM lines ORDER BY id DESC NULLS LAST

WITH lines AS (SELECT id, effective_from, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val) SELECT id, val FROM lines

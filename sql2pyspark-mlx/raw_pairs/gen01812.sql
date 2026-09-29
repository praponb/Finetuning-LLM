WITH exploded AS (SELECT id, effective_from, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val) SELECT id FROM exploded;

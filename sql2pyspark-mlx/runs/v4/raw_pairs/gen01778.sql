WITH lines AS (SELECT id, effective_from, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) x AS val) SELECT effective_from, val FROM lines ORDER BY effective_from DESC;

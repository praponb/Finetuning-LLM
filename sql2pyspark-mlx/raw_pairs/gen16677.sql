WITH lines AS (SELECT id, plan, effective_from, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val) SELECT effective_from, val FROM lines ORDER BY val, effective_from;

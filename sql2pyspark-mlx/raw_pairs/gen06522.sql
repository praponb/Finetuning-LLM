SELECT id, effective_from, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) e AS val;

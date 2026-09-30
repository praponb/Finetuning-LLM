SELECT id, plan, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) li AS val;

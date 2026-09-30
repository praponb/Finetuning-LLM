SELECT id, plan, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) x AS val

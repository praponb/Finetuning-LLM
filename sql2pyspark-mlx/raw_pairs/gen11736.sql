SELECT id, plan, currency, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) t AS val;

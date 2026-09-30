SELECT id, effective_from, currency, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val

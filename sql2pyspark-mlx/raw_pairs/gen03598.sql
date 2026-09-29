SELECT id, plan, currency, element FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element

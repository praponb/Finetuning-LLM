SELECT id, effective_from, currency, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) t AS tier_value;

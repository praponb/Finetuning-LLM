SELECT id, plan, effective_from, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_prices) e AS tier_value;

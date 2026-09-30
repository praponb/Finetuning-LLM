SELECT id, currency, tier_value FROM price_tiers LATERAL VIEW EXPLODE(discounts) t AS tier_value;

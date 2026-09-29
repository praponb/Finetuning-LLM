WITH flattened AS (SELECT id, tier_value FROM price_tiers LATERAL VIEW EXPLODE(discounts) e AS tier_value) SELECT id, tier_value FROM flattened;

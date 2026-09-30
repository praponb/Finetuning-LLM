SELECT id, EXPLODE(tier_limits) AS tier_value FROM price_tiers;

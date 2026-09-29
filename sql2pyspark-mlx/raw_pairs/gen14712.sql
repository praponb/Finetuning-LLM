SELECT id, currency, EXPLODE(tier_prices) AS val FROM price_tiers;

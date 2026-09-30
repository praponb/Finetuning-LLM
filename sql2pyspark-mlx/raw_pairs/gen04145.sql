select id, plan, effective_from, currency, explode(tier_prices) as element from price_tiers;

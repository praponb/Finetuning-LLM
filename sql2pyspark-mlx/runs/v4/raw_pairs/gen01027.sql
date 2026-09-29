select id, plan, currency, explode_outer(discounts) as element from price_tiers;

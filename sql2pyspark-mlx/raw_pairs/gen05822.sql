select id, plan, effective_from, currency, element from price_tiers lateral view outer explode(tier_limits) e as element;

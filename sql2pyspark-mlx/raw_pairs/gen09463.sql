select id, plan, currency, tier_value from price_tiers lateral view explode(discounts) t as tier_value;

with exploded as (select id, plan, tier_value from price_tiers lateral view explode(discounts) x as tier_value) select id, plan from exploded;

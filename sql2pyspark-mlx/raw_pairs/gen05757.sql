with exploded as (select id, plan, tier_value from price_tiers lateral view explode(tier_limits) e as tier_value) select id, tier_value from exploded order by tier_value desc

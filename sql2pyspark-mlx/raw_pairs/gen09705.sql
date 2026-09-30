with exploded as (select id, effective_from, val from price_tiers lateral view explode(tier_prices) e as val) select val from exploded order by val desc

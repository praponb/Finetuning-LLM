with exploded as (select id, plan, effective_from, explode_outer(discounts) as val from price_tiers) select id, plan, sum(val) over (partition by plan) as plan_sum_val from exploded where val <= 72

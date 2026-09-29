with flattened as (select id, plan, currency, explode_outer(discounts) as val from price_tiers) select id, plan, currency from flattened order by plan desc, id

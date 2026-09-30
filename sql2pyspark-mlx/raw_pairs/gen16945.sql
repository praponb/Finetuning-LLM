with flattened as (select id, plan, effective_from, explode_outer(tier_limits) as val from price_tiers) select id, effective_from, val from flattened order by effective_from asc;

with exploded as (
    select
        id,
        plan,
        explode_outer(tier_limits) as element
    from price_tiers
),
enriched as (
    select
        *,
        dense_rank() over (partition by plan order by element desc nulls last) as element_dense_rank,
        cume_dist() over (partition by plan order by element desc nulls last) as element_cume_dist,
        dense_rank() over (order by element desc nulls first) as element_dense_rank_2
    from exploded
),
scored as (
    select
        *,
        max(element_dense_rank_2) over (partition by plan) as plan_max_element_dense_rank_2,
        max(element_cume_dist) over (partition by plan) as plan_max_element_cume_dist,
        cume_dist() over (partition by plan order by element_dense_rank_2 nulls first) as element_dense_rank_2_cume_dist
    from enriched
    where plan <> 'pro'
),
scored_price_tiers as (
    select *, rank() over (order by plan_max_element_dense_rank_2 desc nulls first) as plan_max_element_dense_rank_2_rank
    from scored
),
cleaned as (
    select
        plan,
        element,
        element_dense_rank,
        element_cume_dist,
        element_dense_rank_2,
        plan_max_element_cume_dist,
        element_dense_rank_2_cume_dist,
        plan_max_element_dense_rank_2_rank,
        plan_max_element_dense_rank_2_rank * element as plan_max_element_dense_rank_2_rank_times_element
    from scored_price_tiers
    where plan in ('enterprise', 'starter', 'pro')
)
select
    plan,
    element,
    element_dense_rank,
    element_cume_dist,
    plan_max_element_cume_dist,
    element_dense_rank_2_cume_dist,
    plan_max_element_dense_rank_2_rank,
    plan_max_element_dense_rank_2_rank_times_element,
    case when plan rlike '^(enterprise|basic)$' then 'EXTENDED' when plan rlike '^(starter|pro)$' then 'CORE' else 'OTHER_ZONE' end as plan_zone
from cleaned;

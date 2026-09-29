with lines as (
    select
        id,
        plan,
        effective_from,
        element
    from price_tiers
    lateral view outer explode(discounts) e as element
),
enriched as (
    select *, cume_dist() over (partition by plan order by element asc nulls last) element_cume_dist
    from lines
),
with_windows as (
    select
        *,
        percent_rank() over (partition by plan order by element_cume_dist desc nulls last) element_cume_dist_percent_rank,
        avg(element_cume_dist) over (partition by plan) plan_avg_element_cume_dist
    from enriched
    where plan = 'starter'
),
filtered as (
    select
        id,
        plan,
        effective_from,
        element,
        element_cume_dist_percent_rank,
        plan_avg_element_cume_dist,
        plan_avg_element_cume_dist - element plan_avg_element_cume_dist_minus_element
    from with_windows
    where effective_from >= to_date('2024-04-09')
),
remaining_keys as (
    select id, plan
    from filtered
    except
    select id, plan
    from price_tiers
    where effective_from > '2025-01-07'
),
reconciled as (
    select s.*
    from filtered s
    inner join remaining_keys k on s.id = k.id and s.plan = k.plan
)
select
    plan,
    case when plan_avg_element_cume_dist_minus_element >= 66 then 'PREMIUM' else 'STANDARD' end as plan_avg_element_cume_dist_minus_element_tier,
    size(collect_set(plan)) as num_plans,
    round(avg(plan_avg_element_cume_dist_minus_element), 2) as avg_plan_avg_element_cume_dist_minus_element,
    round(avg(plan_avg_element_cume_dist_minus_element), 2) as avg_plan_avg_element_cume_dist_minus_element_2,
    sum(case when plan = 'basic' then element_cume_dist_percent_rank else 0 end) as basic_element_cume_dist_percent_rank,
    approx_percentile(element_cume_dist_percent_rank, 0.95) as element_cume_dist_percent_rank_p95,
    count(distinct plan) as distinct_plans
from reconciled
group by cube (plan, case when plan_avg_element_cume_dist_minus_element >= 66 then 'PREMIUM' else 'STANDARD' end)

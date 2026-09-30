with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode(discounts) as element
    from price_tiers
),
base as (
    -- Basic cleansing and defaults
    select *, trim(plan) as clean_plan
    from lines
    where plan in ('enterprise', 'starter', 'basic')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        clean_plan,
        min(element) over (partition by plan) as plan_min_element,
        rank() over (order by element desc nulls first) as element_rank,
        sum(element) over (partition by plan) as plan_sum_element,
        cume_dist() over (partition by plan order by element desc nulls last) as element_cume_dist,
        avg(element) over (partition by plan) as plan_avg_element
    from base
    where element between 27 and 51
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        clean_plan,
        plan_min_element,
        plan_avg_element,
        percent_rank() over (order by plan_min_element) as plan_min_element_percent_rank,
        percent_rank() over (order by plan_min_element nulls last) as plan_min_element_percent_rank_2
    from with_windows
)
select
    clean_plan,
    plan_min_element,
    plan_avg_element,
    plan_min_element_percent_rank,
    plan_min_element_percent_rank_2
from metrics
order by plan_min_element_percent_rank nulls first, plan_min_element_percent_rank_2;

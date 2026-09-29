with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        explode_outer(tier_limits) as val
    from price_tiers
),
enriched as (
    -- Ranking and gap features
    select
        *,
        max(val) over w_hist as plan_max_val,
        sum(val) over w_hist as plan_sum_val,
        rank() over (order by val desc) as val_rank,
        count(val) over w_hist as plan_count_val,
        rank() over (partition by plan order by val desc nulls last) as val_rank_2
    from exploded
    window w_hist as (partition by plan)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        plan_sum_val,
        val_rank,
        plan_count_val,
        case when val >= 89 then 'High' when val >= 24 then 'Medium' else 'Low' end as val_band
    from enriched
    where plan = 'basic'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        plan,
        plan_sum_val,
        plan_count_val,
        case when plan = 'basic' then 1 else 0 end as is_basic,
        upper(plan) as plan_upper
    from filtered
    where plan = 'basic'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        rank() over w_ordered as plan_sum_val_rank,
        dense_rank() over w_hist as plan_sum_val_dense_rank,
        min(is_basic) over (partition by plan) as plan_min_is_basic,
        rank() over w_running as plan_count_val_rank
    from base
    window w_ordered as (partition by plan order by plan_sum_val desc nulls last), w_hist as (order by plan_sum_val asc nulls last), w_running as (partition by plan order by plan_count_val desc nulls first)
),
metrics as (
    -- Ranking and gap features
    select
        plan,
        plan_sum_val,
        plan_count_val,
        plan_upper,
        plan_sum_val_rank,
        plan_sum_val_dense_rank,
        plan_count_val_rank,
        sum(is_basic) over (partition by plan) as plan_sum_is_basic,
        cume_dist() over (partition by plan order by plan_sum_val desc nulls first) as plan_sum_val_cume_dist,
        dense_rank() over (partition by plan order by plan_min_is_basic asc nulls last) as plan_min_is_basic_dense_rank
    from scored
)
-- Multi-level aggregation
select
    case when plan_count_val >= 16 then 'PREMIUM' else 'STANDARD' end as plan_count_val_tier,
    plan,
    sort_array(collect_set(plan)) as plan_list,
    percentile_approx(plan_count_val, 0.95) as plan_count_val_p95,
    count(*) as event_count,
    count(distinct plan) as distinct_plans
from metrics
group by case when plan_count_val >= 16 then 'PREMIUM' else 'STANDARD' end, plan;

with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        effective_from,
        currency,
        explode(discounts) as val
    from price_tiers
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        effective_from,
        currency,
        val,
        coalesce(val, 0) as val_filled
    from flattened
    where effective_from <= '2024-04-29'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        plan,
        effective_from,
        currency,
        val_filled,
        cume_dist() over (partition by date_trunc('week', effective_from), currency order by val desc nulls last) as val_cume_dist,
        dense_rank() over (partition by plan, currency order by val nulls first) as val_dense_rank,
        rank() over w_running as val_rank,
        percent_rank() over (partition by plan order by val desc nulls first) as val_percent_rank
    from valid
    window w_running as (order by val nulls last)
),
cleaned as (
    -- Keep recent, valid rows
    select
        plan,
        currency,
        val_filled,
        val_cume_dist,
        val_dense_rank,
        round(val_dense_rank * 1.15, 2) as adjusted_val_dense_rank,
        substring(plan, 1, 2) as plan_prefix
    from with_windows
    where val_dense_rank between 3 and 5
),
enriched as (
    -- Ranking and gap features
    select
        plan,
        currency,
        adjusted_val_dense_rank,
        cume_dist() over (partition by currency, plan order by val_cume_dist desc nulls first) as val_cume_dist_cume_dist,
        count(val_dense_rank) over (partition by plan) as plan_count_val_dense_rank,
        count(val_filled) over (partition by plan) as plan_count_val_filled
    from cleaned
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(adjusted_val_dense_rank) over (partition by plan) as plan_avg_adjusted_val_dense_rank,
        min(adjusted_val_dense_rank) over (partition by plan) as plan_min_adjusted_val_dense_rank,
        round(plan_count_val_dense_rank / sum(plan_count_val_dense_rank) over (partition by currency), 4) as plan_count_val_dense_rank_share,
        percent_rank() over (partition by plan order by plan_count_val_filled desc nulls last) as plan_count_val_filled_percent_rank,
        cume_dist() over (order by plan_count_val_filled nulls last) as plan_count_val_filled_cume_dist
    from enriched
    where adjusted_val_dense_rank <= 30
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        greatest(plan_count_val_filled_percent_rank, plan_count_val_filled_cume_dist) as greatest_plan_count_val_filled_percent_rank_plan_count_val_filled_cume_dist,
        plan_count_val_filled_cume_dist / nullif(val_cume_dist_cume_dist, 0) as plan_count_val_filled_cume_dist_per_val_cume_dist_cume_dist
    from scored
    where plan_count_val_filled < 7
),
stats as (
    -- Multi-level aggregation
    select
        plan,
        currency,
        count(case when currency = 'USD' then 1 end) as usd_count,
        count_if(plan = 'enterprise') as enterprise_events,
        sort_array(collect_set(plan)) as plan_list,
        count(*) filter (where currency = 'USD') as usd_rows,
        max(plan_count_val_dense_rank_share) filter (where plan = 'basic') as basic_max_plan_count_val_dense_rank_share,
        approx_percentile(plan_count_val_filled_percent_rank, array(0.25, 0.5, 0.75)) as plan_count_val_filled_percent_rank_quartiles
    from filtered
    group by plan, currency
)
select
    plan,
    currency,
    usd_count,
    enterprise_events,
    plan_list,
    basic_max_plan_count_val_dense_rank_share,
    plan_count_val_filled_percent_rank_quartiles
from stats
order by plan desc, currency desc;

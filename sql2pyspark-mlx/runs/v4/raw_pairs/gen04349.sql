with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode(tier_limits) as tier_value
    from price_tiers
),
metrics as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (order by tier_value desc) as tier_value_cume_dist,
        sum(tier_value) over (partition by currency) as currency_sum_tier_value,
        rank() over (order by tier_value desc nulls last) as tier_value_rank,
        avg(tier_value) over (partition by plan) as plan_avg_tier_value
    from exploded
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        min(tier_value_cume_dist) over (partition by currency) as currency_min_tier_value_cume_dist,
        max(currency_sum_tier_value) over (partition by plan, currency) as plan_max_currency_sum_tier_value,
        rank() over (partition by currency, plan order by currency_sum_tier_value asc nulls last) as currency_sum_tier_value_rank
    from metrics
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by plan order by plan_max_currency_sum_tier_value desc) as plan_max_currency_sum_tier_value_rank,
        cume_dist() over (order by currency_sum_tier_value desc nulls last) as currency_sum_tier_value_cume_dist,
        max(plan_max_currency_sum_tier_value) over (partition by currency, plan) as currency_max_plan_max_currency_sum_tier_value,
        rank() over (partition by plan order by plan_max_currency_sum_tier_value asc nulls first) as plan_max_currency_sum_tier_value_rank_2,
        sum(plan_avg_tier_value) over (partition by currency) as currency_sum_plan_avg_tier_value
    from enriched
),
with_windows_price_tiers as (
    -- Ranking and gap features
    select
        id,
        plan,
        currency,
        tier_value,
        tier_value_cume_dist,
        currency_sum_tier_value,
        tier_value_rank,
        plan_avg_tier_value,
        currency_min_tier_value_cume_dist,
        plan_max_currency_sum_tier_value,
        currency_sum_tier_value_rank,
        plan_max_currency_sum_tier_value_rank,
        currency_sum_tier_value_cume_dist,
        currency_max_plan_max_currency_sum_tier_value,
        plan_max_currency_sum_tier_value_rank_2,
        currency_sum_plan_avg_tier_value,
        max(currency_sum_tier_value_cume_dist) over (partition by currency) as currency_max_currency_sum_tier_value_cume_dist,
        avg(tier_value_cume_dist) over (partition by plan) as plan_avg_tier_value_cume_dist,
        rank() over (partition by currency order by currency_max_plan_max_currency_sum_tier_value desc) as currency_max_plan_max_currency_sum_tier_value_rank,
        sum(currency_sum_tier_value) over (partition by currency) as currency_sum_currency_sum_tier_value,
        dense_rank() over (partition by currency order by plan_max_currency_sum_tier_value_rank desc nulls last) as plan_max_currency_sum_tier_value_rank_dense_rank
    from with_windows
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(currency_sum_tier_value, 1) as currency_sum_tier_value_rounded,
        cast(plan_max_currency_sum_tier_value_rank as double) as plan_max_currency_sum_tier_value_rank_num
    from with_windows_price_tiers
    where currency not in ('EUR', 'USD', 'THB')
),
metrics_price_tiers as (
    -- Ranking and gap features
    select
        id,
        plan,
        currency,
        tier_value,
        tier_value_cume_dist,
        currency_sum_tier_value,
        tier_value_rank,
        plan_avg_tier_value,
        currency_min_tier_value_cume_dist,
        plan_max_currency_sum_tier_value,
        currency_sum_tier_value_rank,
        plan_max_currency_sum_tier_value_rank,
        currency_sum_tier_value_cume_dist,
        currency_max_plan_max_currency_sum_tier_value,
        plan_max_currency_sum_tier_value_rank_2,
        currency_sum_plan_avg_tier_value,
        currency_max_currency_sum_tier_value_cume_dist,
        plan_avg_tier_value_cume_dist,
        currency_max_plan_max_currency_sum_tier_value_rank,
        currency_sum_currency_sum_tier_value,
        plan_max_currency_sum_tier_value_rank_dense_rank,
        currency_sum_tier_value_rounded,
        plan_max_currency_sum_tier_value_rank_num,
        stddev_samp(plan_avg_tier_value_cume_dist) over (partition by plan) as plan_stddev_samp_plan_avg_tier_value_cume_dist,
        sum(plan_avg_tier_value_cume_dist) over w_part as currency_sum_plan_avg_tier_value_cume_dist
    from valid
    window w_part as (partition by currency)
),
stats as (
    -- Multi-level aggregation
    select
        currency,
        case when currency_sum_plan_avg_tier_value >= 324 then 'LARGE' else 'SMALL' end as currency_sum_plan_avg_tier_value_tier,
        plan,
        count(case when currency = 'USD' then 1 end) as usd_count,
        sum(plan_avg_tier_value) filter (where currency = 'EUR') as eur_total_plan_avg_tier_value,
        count(case when plan = 'pro' then 1 end) as pro_count,
        round(sum(currency_max_plan_max_currency_sum_tier_value_rank) / nullif(count(case when plan = 'enterprise' then 1 end), 0), 2) as currency_max_plan_max_currency_sum_tier_value_rank_per_enterprise
    from metrics_price_tiers
    group by cube (currency, case when currency_sum_plan_avg_tier_value >= 324 then 'LARGE' else 'SMALL' end, plan)
)
select
    currency_sum_plan_avg_tier_value_tier,
    plan,
    usd_count
from stats
order by plan asc;

with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        tier_value
    from price_tiers
    lateral view explode(tier_limits) t as tier_value
),
scored as (
    -- Running and moving aggregates
    select *, percent_rank() over (partition by plan order by tier_value) as tier_value_percent_rank
    from lines
),
enriched as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(tier_value_percent_rank) over (partition by plan) as plan_stddev_samp_tier_value_percent_rank,
        percent_rank() over (partition by plan order by tier_value desc nulls first) as tier_value_percent_rank_2
    from scored
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(tier_value_percent_rank) as tier_value_percent_rank_whole
    from enriched
    where plan in ('basic', 'starter', 'pro')
),
scored_price_tiers as (
    -- Ranking and gap features
    select
        id,
        plan,
        count(tier_value) over (partition by plan) as plan_count_tier_value,
        dense_rank() over w_part as tier_value_percent_rank_2_dense_rank,
        rank() over w_running as tier_value_percent_rank_2_rank
    from cleaned
    window w_part as (partition by plan order by tier_value_percent_rank_2 desc), w_running as (order by tier_value_percent_rank_2 desc nulls first)
)
select
    id,
    plan_count_tier_value,
    tier_value_percent_rank_2_rank
from scored_price_tiers
order by id

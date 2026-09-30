with flattened as (
    -- Unnest the array into one row per element
    select id, explode_outer(tier_prices) as tier_value
    from price_tiers
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        tier_value,
        rank() over (order by tier_value desc) as tier_value_rank,
        rank() over (order by tier_value desc) as tier_value_rank_2,
        cume_dist() over (order by tier_value desc nulls first) as tier_value_cume_dist,
        dense_rank() over (order by tier_value desc nulls first) as tier_value_dense_rank
    from flattened
),
remaining_keys as (
    -- Set difference on keys
    select id
    from with_windows
    except
    select id
    from price_tiers
    where currency in ('USD', 'THB')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join remaining_keys k on s.id = k.id
),
with_windows_price_tiers as (
    -- Window metrics over several different partitions
    select
        id,
        tier_value,
        tier_value_rank,
        tier_value_rank_2,
        tier_value_cume_dist,
        rank() over (order by tier_value_dense_rank desc) as tier_value_dense_rank_rank,
        dense_rank() over (order by tier_value_dense_rank desc) as tier_value_dense_rank_dense_rank,
        cume_dist() over (order by tier_value asc nulls first) as tier_value_cume_dist_2
    from verified
),
scored as (
    -- Running and moving aggregates
    select
        id,
        tier_value_rank,
        tier_value_rank_2,
        tier_value_dense_rank_rank,
        tier_value_cume_dist_2,
        dense_rank() over (order by tier_value asc nulls first) as tier_value_dense_rank,
        dense_rank() over w_running as tier_value_cume_dist_dense_rank,
        dense_rank() over (order by tier_value_rank asc nulls last) as tier_value_rank_dense_rank,
        cume_dist() over (order by tier_value_dense_rank_dense_rank desc) as tier_value_dense_rank_dense_rank_cume_dist,
        percent_rank() over (order by tier_value_dense_rank_rank desc nulls first) as tier_value_dense_rank_rank_percent_rank
    from with_windows_price_tiers
    where tier_value_rank <> 6
    window w_running as (order by tier_value_cume_dist desc nulls first)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        tier_value_rank,
        tier_value_rank_2,
        tier_value_dense_rank,
        tier_value_cume_dist_dense_rank,
        tier_value_dense_rank_rank_percent_rank,
        greatest(tier_value_dense_rank_dense_rank_cume_dist, tier_value_rank_2) as greatest_tier_value_dense_rank_dense_rank_cume_dist_tier_value_rank_2,
        tier_value_dense_rank_rank_percent_rank - tier_value_dense_rank_rank as tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank
    from scored
    where tier_value_rank = 4
)
-- Aggregate with conditional metrics
select
    case when tier_value_dense_rank_rank_percent_rank >= 0.73 then 'HIGH' else 'LOW' end as tier_value_dense_rank_rank_percent_rank_tier,
    case when tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank >= 70 then 'HIGH' else 'LOW' end as tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank_tier,
    case when tier_value_rank_2 >= 8 then 'PREMIUM' else 'STANDARD' end as tier_value_rank_2_tier,
    percentile_approx(tier_value_rank, array(0.25, 0.5, 0.75)) as tier_value_rank_quartiles,
    approx_percentile(tier_value_cume_dist_dense_rank, 0.5) as tier_value_cume_dist_dense_rank_p50,
    percentile_approx(tier_value_cume_dist_dense_rank, 0.5) as tier_value_cume_dist_dense_rank_p50_2,
    rank() over (partition by case when tier_value_dense_rank_rank_percent_rank >= 0.73 then 'HIGH' else 'LOW' end order by approx_percentile(tier_value_cume_dist_dense_rank, 0.5) desc) as tier_value_cume_dist_dense_rank_p50_rank
from valid
group by tier_value_dense_rank_rank_percent_rank_tier, tier_value_dense_rank_rank_percent_rank_minus_tier_value_dense_rank_rank_tier, tier_value_rank_2_tier

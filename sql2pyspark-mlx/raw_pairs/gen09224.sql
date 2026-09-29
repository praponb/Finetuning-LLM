with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        explode_outer(tier_limits) as val
    from price_tiers
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        effective_from,
        val,
        cume_dist() over w_part as val_cume_dist,
        percent_rank() over (order by val desc nulls first) as val_percent_rank,
        stddev_samp(val) over (partition by date_trunc('WEEK', effective_from)) as effective_from_period_stddev_samp_val,
        cume_dist() over w_part as val_cume_dist_2,
        cume_dist() over w_part as val_cume_dist_3
    from exploded
    window w_part as (order by val)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by date_trunc('WEEK', effective_from) order by val_percent_rank nulls last) as val_percent_rank_cume_dist,
        cume_dist() over (order by val_cume_dist desc nulls first) as val_cume_dist_cume_dist
    from enriched
),
enriched_price_tiers as (
    -- Ranking and gap features
    select
        effective_from,
        val,
        val_percent_rank,
        effective_from_period_stddev_samp_val,
        val_cume_dist_cume_dist,
        dense_rank() over w_recent as val_percent_rank_dense_rank,
        cume_dist() over w_part as effective_from_period_stddev_samp_val_cume_dist
    from scored
    window w_recent as (order by val_percent_rank desc nulls first), w_part as (order by effective_from_period_stddev_samp_val desc nulls last)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by effective_from_period_stddev_samp_val) as effective_from_period_stddev_samp_val_percent_rank,
        dense_rank() over w_part as val_dense_rank,
        rank() over w_part_2 as effective_from_period_stddev_samp_val_cume_dist_rank,
        rank() over (partition by date_trunc('MONTH', effective_from) order by effective_from_period_stddev_samp_val_cume_dist nulls last) as effective_from_period_stddev_samp_val_cume_dist_rank_2
    from enriched_price_tiers
    where val_percent_rank_dense_rank between 3 and 8
    window w_part as (order by val nulls last), w_part_2 as (order by effective_from_period_stddev_samp_val_cume_dist desc nulls first)
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when effective_from_period_stddev_samp_val_cume_dist >= 0.63 then 'HIGH' else 'LOW' end as effective_from_period_stddev_samp_val_cume_dist_tier,
        case when val_cume_dist_cume_dist >= 0.63 then 'LARGE' else 'SMALL' end as val_cume_dist_cume_dist_tier,
        count(*) as num_rows,
        max(effective_from_period_stddev_samp_val_cume_dist_rank_2) as max_effective_from_period_stddev_samp_val_cume_dist_rank_2
    from with_windows
    group by 1, 2
)
select
    effective_from_period_stddev_samp_val_cume_dist_tier,
    val_cume_dist_cume_dist_tier,
    max_effective_from_period_stddev_samp_val_cume_dist_rank_2
from summary
order by effective_from_period_stddev_samp_val_cume_dist_tier desc nulls last

with lines as (
    -- Unnest the array into one row per element
    select
        id,
        plan,
        currency,
        explode_outer(discounts) as val
    from price_tiers
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_hist as val_cume_dist,
        dense_rank() over (partition by currency order by val) as val_dense_rank,
        sum(val) over (partition by currency) as currency_sum_val
    from lines
    window w_hist as (partition by plan order by val desc nulls first)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by currency order by currency_sum_val desc nulls first) as currency_sum_val_percent_rank,
        rank() over (partition by plan order by val_cume_dist nulls last) as val_cume_dist_rank,
        round(val_dense_rank / sum(val_dense_rank) over (partition by plan), 4) as val_dense_rank_share
    from scored
),
metrics_price_tiers as (
    -- Window metrics over several different partitions
    select
        id,
        plan,
        currency,
        val,
        val_cume_dist,
        val_dense_rank,
        currency_sum_val,
        currency_sum_val_percent_rank,
        val_cume_dist_rank,
        val_dense_rank_share,
        rank() over (partition by currency order by currency_sum_val_percent_rank nulls first) as currency_sum_val_percent_rank_rank,
        rank() over (partition by currency order by val desc) as val_rank,
        sum(val_cume_dist) over (partition by currency) as currency_sum_val_cume_dist
    from metrics
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(currency_sum_val) over (partition by plan) as plan_stddev_samp_currency_sum_val,
        percent_rank() over (partition by plan order by currency_sum_val nulls last) as currency_sum_val_percent_rank_2
    from metrics_price_tiers
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        currency,
        val_cume_dist,
        val_dense_rank,
        val_dense_rank_share,
        currency_sum_val_percent_rank_rank,
        val_rank,
        currency_sum_val_cume_dist,
        plan_stddev_samp_currency_sum_val,
        currency_sum_val_percent_rank_2,
        case when currency_sum_val_percent_rank_rank >= 7 then 'High' when currency_sum_val_percent_rank_rank >= 3 then 'Medium' else 'Low' end as currency_sum_val_percent_rank_rank_band,
        ceil(currency_sum_val_percent_rank) as currency_sum_val_percent_rank_whole
    from enriched
    where currency_sum_val_percent_rank <= 0.68
)
-- Multi-level aggregation
select
    case when val_rank >= 9 then 'PREMIUM' else 'STANDARD' end as val_rank_tier,
    percentile_approx(currency_sum_val_percent_rank_rank, 0.9) as currency_sum_val_percent_rank_rank_p90,
    count(distinct currency) as distinct_currencies
from filtered
group by 1
order by currency_sum_val_percent_rank_rank_p90 desc nulls first;

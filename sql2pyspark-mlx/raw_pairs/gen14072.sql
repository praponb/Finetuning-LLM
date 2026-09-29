with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_prices) as tier_value
    from price_tiers
),
valid as (
    -- Keep recent, valid rows
    select *, case when tier_value >= 73 then 'High' when tier_value >= 15 then 'Medium' else 'Low' end as tier_value_band
    from exploded
    where currency in ('THB', 'USD', 'EUR')
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        min(tier_value) over (partition by currency) as currency_min_tier_value,
        percent_rank() over (partition by currency order by tier_value desc nulls first) as tier_value_percent_rank,
        stddev_samp(tier_value) over (partition by currency) as currency_stddev_samp_tier_value
    from valid
),
scored as (
    -- Running and moving aggregates
    select
        id,
        currency,
        tier_value,
        tier_value_band,
        currency_min_tier_value,
        currency_stddev_samp_tier_value,
        percent_rank() over (partition by currency order by currency_min_tier_value nulls last) as currency_min_tier_value_percent_rank,
        dense_rank() over (order by tier_value_percent_rank desc) as tier_value_percent_rank_dense_rank
    from with_windows
),
enriched as (
    -- Ranking and gap features
    select
        id,
        currency,
        tier_value,
        tier_value_band,
        currency_min_tier_value,
        currency_min_tier_value_percent_rank,
        percent_rank() over (order by currency_stddev_samp_tier_value desc nulls first) as currency_stddev_samp_tier_value_percent_rank,
        cume_dist() over (order by currency_min_tier_value desc nulls first) as currency_min_tier_value_cume_dist,
        dense_rank() over (partition by currency order by tier_value_percent_rank_dense_rank desc nulls first) as tier_value_percent_rank_dense_rank_dense_rank
    from scored
),
enriched_price_tiers as (
    -- Ranking and gap features
    select
        tier_value,
        currency_min_tier_value,
        currency_min_tier_value_cume_dist,
        count(tier_value) over w_grp as currency_count_tier_value,
        avg(currency_min_tier_value) over w_grp as currency_avg_currency_min_tier_value,
        percent_rank() over w_running as tier_value_percent_rank,
        percent_rank() over (partition by currency order by currency_min_tier_value desc) as currency_min_tier_value_percent_rank_2,
        cume_dist() over (partition by currency order by currency_stddev_samp_tier_value_percent_rank desc nulls first) as currency_stddev_samp_tier_value_percent_rank_cume_dist
    from enriched
    window w_grp as (partition by currency), w_running as (partition by currency order by tier_value desc nulls last)
)
select
    tier_value,
    currency_min_tier_value,
    currency_min_tier_value_cume_dist,
    currency_avg_currency_min_tier_value,
    currency_min_tier_value_percent_rank_2
from enriched_price_tiers
order by tier_value desc nulls last

with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(tier_limits) as element
    from price_tiers
),
scored as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over w_running element_cume_dist,
        percent_rank() over (order by element nulls first) element_percent_rank,
        count(element) over (partition by currency) currency_count_element,
        rank() over w_running element_rank
    from flattened
    window w_running as (partition by currency order by element nulls first)
),
scored_price_tiers as (
    -- Window metrics over several different partitions
    select *, cume_dist() over (partition by currency order by element_cume_dist nulls first) element_cume_dist_cume_dist
    from scored
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by currency order by element_cume_dist_cume_dist desc) element_cume_dist_cume_dist_percent_rank,
        rank() over (order by element desc) element_rank_2
    from scored_price_tiers
    where element_rank <> 6
),
scored_price_tiers_2 as (
    -- Window metrics over several different partitions
    select
        element_percent_rank,
        currency_count_element,
        element_cume_dist_cume_dist,
        element_cume_dist_cume_dist_percent_rank,
        element_rank_2,
        dense_rank() over (partition by currency order by element_cume_dist_cume_dist_percent_rank desc nulls first) element_cume_dist_cume_dist_percent_rank_dense_rank,
        round(element_rank_2 / sum(element_rank_2) over (partition by currency), 4) element_rank_2_share,
        stddev_samp(element_rank) over (partition by currency) currency_stddev_samp_element_rank,
        dense_rank() over (partition by currency order by element desc nulls first) element_dense_rank,
        round(element_rank / sum(element_rank) over (partition by currency), 4) element_rank_share
    from with_windows
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(element_rank_2_share as int) element_rank_2_share_int,
        element_dense_rank * element_percent_rank element_dense_rank_times_element_percent_rank
    from scored_price_tiers_2
    where element_cume_dist_cume_dist_percent_rank_dense_rank > 6
)
-- Aggregate with conditional metrics
select
    case when element_rank_2_share_int >= 78 then 'HIGH' else 'LOW' end as element_rank_2_share_int_tier,
    case when currency_stddev_samp_element_rank >= 8 then 'PREMIUM' else 'STANDARD' end as currency_stddev_samp_element_rank_tier,
    case when element_percent_rank >= 0.61 then 'LARGE' else 'SMALL' end as element_percent_rank_tier,
    round(avg(element_rank_2), 2) as avg_element_rank_2,
    count(*) as event_count,
    round(avg(element_rank_share), 2) as avg_element_rank_share,
    round(avg(currency_stddev_samp_element_rank), 2) as avg_currency_stddev_samp_element_rank,
    round(avg(element_rank_2), 2) as avg_element_rank_2_2
from filtered
group by case when element_rank_2_share_int >= 78 then 'HIGH' else 'LOW' end, case when currency_stddev_samp_element_rank >= 8 then 'PREMIUM' else 'STANDARD' end, case when element_percent_rank >= 0.61 then 'LARGE' else 'SMALL' end;

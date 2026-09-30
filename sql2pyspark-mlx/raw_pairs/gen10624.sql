with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) t as element
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        currency,
        percent_rank() over (partition by currency order by element desc) as element_percent_rank,
        avg(element) over (partition by currency) as currency_avg_element
    from exploded
    where currency <> 'EUR'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(currency_avg_element * 0.85, 2) as currency_avg_element_adjusted
    from metrics
    where currency = 'USD'
),
enriched as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by currency order by element_percent_rank desc nulls last) as element_percent_rank_rank,
        cume_dist() over w_recent as element_percent_rank_cume_dist,
        stddev_samp(currency_avg_element) over (partition by currency) as currency_stddev_samp_currency_avg_element
    from valid
    window w_recent as (partition by currency order by element_percent_rank nulls first)
)
-- Window metrics over several different partitions
select
    id,
    element_percent_rank,
    currency_avg_element,
    element_percent_rank_cume_dist,
    rank() over (partition by currency order by element_percent_rank nulls last) as element_percent_rank_rank_2,
    percent_rank() over (partition by currency order by element_percent_rank_rank) as element_percent_rank_rank_percent_rank
from enriched;

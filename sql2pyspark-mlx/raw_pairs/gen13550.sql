with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        currency,
        explode_outer(discounts) as val
    from price_tiers
)
-- Running and moving aggregates
select
    currency,
    val,
    percent_rank() over (order by val desc nulls last) as val_percent_rank,
    sum(val) over (partition by currency) as currency_sum_val,
    percent_rank() over (partition by currency order by val desc nulls first) as val_percent_rank_2,
    dense_rank() over (order by val desc) as val_dense_rank,
    min(val) over (partition by currency) as currency_min_val
from flattened;

with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode(discounts) as val
    from price_tiers
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by date_trunc('YEAR', effective_from), currency order by val desc nulls first) as val_rank,
        percent_rank() over w_recent as val_percent_rank,
        dense_rank() over w_recent as val_dense_rank,
        rank() over (partition by currency order by val desc nulls first) as val_rank_2,
        dense_rank() over (order by val desc) as val_dense_rank_2
    from lines
    window w_recent as (partition by currency order by val)
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        effective_from,
        val_rank,
        val_percent_rank,
        val_dense_rank_2,
        cume_dist() over (partition by year(effective_from), currency order by val_dense_rank_2 desc nulls first) as val_dense_rank_2_cume_dist,
        percent_rank() over (order by val_rank desc) as val_rank_percent_rank
    from with_windows
    where val_rank_2 between 4 and 8
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, val_dense_rank_2 / nullif(val_rank_percent_rank, 0) as val_dense_rank_2_per_val_rank_percent_rank
    from enriched
    where val_rank = 4
),
filtered as (
    -- Keep recent, valid rows
    select *, cast(val_dense_rank_2_per_val_rank_percent_rank as int) as val_dense_rank_2_per_val_rank_percent_rank_int
    from cleaned
    where effective_from >= date '2024-05-19'
),
enriched_price_tiers as (
    -- Ranking and gap features
    select *, dense_rank() over (order by val_rank_percent_rank desc nulls last) as val_rank_percent_rank_dense_rank
    from filtered
),
scored as (
    -- Ranking and gap features
    select
        id,
        effective_from,
        val_rank,
        val_dense_rank_2_cume_dist,
        val_dense_rank_2_per_val_rank_percent_rank,
        rank() over (order by val_rank_percent_rank_dense_rank) as val_rank_percent_rank_dense_rank_rank,
        cume_dist() over (order by val_rank_percent_rank_dense_rank) as val_rank_percent_rank_dense_rank_cume_dist
    from enriched_price_tiers
)
select
    effective_from,
    val_rank,
    val_dense_rank_2_per_val_rank_percent_rank,
    val_rank_percent_rank_dense_rank_rank,
    val_rank_percent_rank_dense_rank_cume_dist
from scored
order by val_rank_percent_rank_dense_rank_cume_dist nulls first;

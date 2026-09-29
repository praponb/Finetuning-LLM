with lines as (
    -- Unnest the array into one row per element
    select
        id,
        effective_from,
        currency,
        explode(tier_limits) as val
    from price_tiers
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, length(currency) as currency_length
    from lines
    where effective_from < '2024-10-28'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(val as string) as val_str
    from cleaned
    where year(effective_from) >= 2024
),
with_windows as (
    -- Window metrics over several different partitions
    select
        effective_from,
        val,
        currency_length,
        percent_rank() over (partition by currency order by currency_length) as currency_length_percent_rank,
        dense_rank() over (order by currency_length nulls first) as currency_length_dense_rank
    from filtered
    where val < 65
),
with_windows_price_tiers as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by currency_length_dense_rank desc nulls first) as currency_length_dense_rank_percent_rank,
        percent_rank() over w_grp as currency_length_percent_rank_2,
        dense_rank() over (order by currency_length_percent_rank desc) as currency_length_percent_rank_dense_rank,
        avg(val) over w_part as effective_from_year_avg_val,
        rank() over (order by val) as val_rank
    from with_windows
    window w_grp as (order by currency_length desc), w_part as (partition by year(effective_from))
),
totals as (
    -- Aggregate with conditional metrics
    select
        date_trunc('quarter', effective_from) as quarter_start,
        percentile_approx(currency_length_dense_rank_percent_rank, 0.95) as currency_length_dense_rank_percent_rank_p95,
        percentile_approx(currency_length_percent_rank_2, 0.9) as currency_length_percent_rank_2_p90,
        percentile_approx(val_rank, array(0.25, 0.5, 0.75)) as val_rank_quartiles
    from with_windows_price_tiers
    group by date_trunc('quarter', effective_from)
)
select
    quarter_start,
    currency_length_percent_rank_2_p90,
    val_rank_quartiles
from totals
order by quarter_start desc;

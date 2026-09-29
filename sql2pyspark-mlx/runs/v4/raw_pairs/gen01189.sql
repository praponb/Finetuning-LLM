with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        units,
        revenue,
        date_trunc('month', sale_date) as sale_date_period
    from store_sales
    where store_id in (3, 5)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (order by revenue, id) as row_seq,
        rank() over (order by units desc nulls last) as units_rank,
        max(units) over (order by cast(sale_date_period as bigint) range between 2592000 preceding and current row) as units_max_30d
    from cleaned
),
enriched_store_sales as (
    -- Window metrics over several different partitions
    select
        id,
        units,
        revenue,
        sale_date_period,
        row_seq,
        units_rank,
        units_max_30d,
        lead(units_rank, 2) over (order by row_seq, id) as next_units_rank,
        last_value(units, true) over (order by row_seq, id rows between unbounded preceding and current row) as last_known_units,
        ntile(4) over (order by units_max_30d, id) as units_max_30d_bucket,
        ntile(5) over (order by row_seq, id) as row_seq_bucket
    from enriched
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        units,
        revenue,
        sale_date_period,
        row_seq,
        units_rank,
        units_max_30d,
        next_units_rank,
        last_known_units,
        units_max_30d_bucket,
        row_seq_bucket,
        sum(units_rank) over (order by sale_date_period, id rows between unbounded preceding and current row) as cumulative_units_rank,
        first_value(last_known_units, true) over (order by sale_date_period, id rows between unbounded preceding and current row) as first_known_last_known_units,
        avg(units) over (order by row_seq_bucket, id rows between 3 preceding and 1 preceding) as units_avg_3_prior,
        sum(last_known_units) over (order by next_units_rank, id rows between unbounded preceding and current row) as cumulative_last_known_units
    from enriched_store_sales
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (order by revenue) as revenue_percent_rank,
        ntile(10) over (order by cumulative_units_rank desc, id) as cumulative_units_rank_bucket
    from with_windows
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        revenue_percent_rank,
        revenue
    from metrics
) src
unpivot include nulls (
    metric_value for measure in (revenue_percent_rank, revenue as REVENUE)
)

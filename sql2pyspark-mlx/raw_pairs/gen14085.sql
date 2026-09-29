with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, day(sale_date) as sale_date_day
    from store_sales
    where sale_date > date '2023-09-26'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by month(sale_date) order by revenue, id) as event_seq,
        lead(sale_date_day, 1) over (partition by store_id order by sale_date_day, id) as next_sale_date_day
    from filtered
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by store_id order by sale_date desc nulls last) as rank_in_group
    from with_windows
    qualify rank_in_group = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by sale_date desc, id) as latest_rank
        from ranked
    ) x
    where latest_rank = 1
)
-- Aggregate with conditional metrics
select
    product_id,
    store_id,
    count(*) as num_rows,
    percentile_approx(revenue, array(0.25, 0.5, 0.75)) as revenue_quartiles,
    count(*) as event_count
from latest
group by cube (product_id, store_id)

with scored as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        round(revenue / sum(revenue) over (partition by date_trunc('month', sale_date)), 4) as revenue_share,
        percent_rank() over (partition by store_id order by units nulls last) as units_percent_rank
    from store_sales
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, ceil(revenue) as revenue_whole
    from scored
    where sale_date >= to_date('2023-05-12')
),
enriched as (
    -- Attach products attributes
    select
        s.id,
        s.store_id,
        s.product_id,
        s.units_percent_rank,
        b.launch_date
    from cleaned s
    left join products b
        on s.product_id = b.id
    where b.cost between 190 and 490
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(units_percent_rank) over (partition by product_id order by units_percent_rank, id rows between 6 preceding and current row) as units_percent_rank_avg_6,
        lag(units_percent_rank, 2) over (partition by month(launch_date), product_id order by launch_date, id) as prev_units_percent_rank,
        row_number() over (partition by product_id order by launch_date desc, id) as row_seq,
        sum(units_percent_rank) over (partition by year(launch_date), product_id order by units_percent_rank, id rows between unbounded preceding and current row) as running_units_percent_rank,
        sum(units_percent_rank) over (order by units_percent_rank, id rows between unbounded preceding and current row) as cumulative_units_percent_rank
    from enriched
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_windows
    qualify row_number() over (partition by product_id order by units_percent_rank_avg_6 desc, id) = 1
)
-- Join a dimension and aggregate
select
    p.category,
    count(*) as rows_total,
    avg(a.prev_units_percent_rank) as avg_prev_units_percent_rank
from latest a
left join products p on a.product_id = p.id
group by rollup (p.category)
order by category desc;

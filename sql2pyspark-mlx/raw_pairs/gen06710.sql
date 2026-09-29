with base as (
    -- Basic cleansing and defaults
    select *, least(units, revenue) as least_units_revenue
    from store_sales
    where product_id in (2, 10)
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        last_day(sale_date) as sale_date_month_end
    from base
    where sale_date >= date '2023-04-26'
),
scored as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        sale_date_month_end,
        lead(revenue) over (partition by store_id order by revenue asc, id asc) as next_revenue,
        lag(units) over (partition by store_id order by revenue asc, id asc) as prev_units,
        lead(units) over (partition by store_id order by revenue asc, id asc) as next_units,
        row_number() over (partition by store_id order by revenue asc, id asc) as row_seq,
        row_number() over (partition by month(sale_date) order by units asc, id asc) as seq_no
    from filtered
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id order by next_units asc nulls last, id asc) as latest_rank
        from scored
    ) ranked
    where latest_rank = 1
)
-- Join a dimension and aggregate
select
    d.region,
    count(*) as rows_total,
    min(e.next_revenue) as min_next_revenue
from latest e
inner join stores d on e.store_id = d.id
group by d.region
order by region asc nulls last;

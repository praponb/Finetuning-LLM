with with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(units, 1) over (partition by store_id, product_id order by sale_date, id) as prev_units,
        last_value(revenue, true) over (order by revenue, id rows between unbounded preceding and current row) as last_known_revenue,
        avg(units) over (order by revenue, id rows between 3 preceding and current row) as units_avg_3,
        avg(units) over (order by units, id rows between 2 preceding and current row) as units_avg_2
    from store_sales
),
enriched as (
    -- Attach products attributes
    select
        a.id,
        a.store_id,
        a.product_id,
        a.prev_units,
        a.units_avg_3,
        a.units_avg_2,
        d.category
    from with_windows a
    inner join products d
        on a.product_id = d.id
    where d.brand not in ('Globex', 'Stark', 'Initech')
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by product_id order by prev_units desc nulls first) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
latest_store_sales as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by store_id, category order by dedup_rank desc) as rn
    from latest
    qualify rn = 1
),
latest_store_sales_2 as (
    -- Keep one row per key (latest first)
    select *
    from latest_store_sales
    qualify row_number() over (partition by store_id order by units_avg_2 desc, id) = 1
)
-- Aggregate with conditional metrics
select
    store_id,
    category,
    count(*) as event_count,
    avg(units_avg_2) as avg_units_avg_2,
    count_if(category = 'Home') as home_events,
    sum(case when category = 'Grocery' then units_avg_3 else 0 end) as grocery_units_avg_3,
    percent_rank() over (partition by store_id order by sum(case when category = 'Grocery' then units_avg_3 else 0 end) desc) as grocery_units_avg_3_percent_rank
from latest_store_sales_2
group by store_id, category

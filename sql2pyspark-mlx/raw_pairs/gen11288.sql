with scored as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        min(units) over w_ordered as running_min_units,
        sum(units) over w_ordered as running_sum_units,
        max(units) over w_ordered as running_max_units,
        last_value(units) ignore nulls over (order by units, id rows between unbounded preceding and current row) as last_known_units
    from store_sales
    where revenue >= 1200
    window w_ordered as (partition by product_id order by units, id rows between unbounded preceding and current row)
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(revenue / units, 2) as revenue_per_unit,
        least(running_max_units, revenue) as least_running_max_units_revenue
    from scored
    where month(sale_date) in (3, 4, 12)
),
scored_store_sales as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        revenue,
        running_sum_units,
        last_known_units,
        least_running_max_units_revenue,
        row_number() over (partition by month(sale_date), store_id order by running_min_units desc, id) as event_seq,
        ntile(5) over (order by running_max_units desc, id) as running_max_units_bucket
    from valid
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        revenue,
        least_running_max_units_revenue,
        event_seq,
        running_max_units_bucket,
        ntile(10) over (partition by store_id, product_id order by last_known_units desc, id) as last_known_units_bucket,
        sum(least_running_max_units_revenue) over (partition by product_id order by revenue, id rows between unbounded preceding and current row) as running_least_running_max_units_revenue
    from scored_store_sales
)
-- Join a dimension and aggregate
select
    d.brand,
    d.category,
    count(*) as num_rows,
    min(a.revenue) as min_revenue,
    round(avg(a.running_max_units_bucket), 2) as avg_running_max_units_bucket,
    grouping_id() as gid
from enriched a
left join products d on a.product_id = d.id
group by grouping sets ((d.brand, d.category), (d.brand), ())
order by category nulls last;

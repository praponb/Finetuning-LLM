with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from products s
    where exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.last_restock >= s.launch_date
        )
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from kept
    qualify rank() over (partition by brand order by launch_date nulls last) = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(3) over (partition by supplier_id order by stock_qty, id) as stock_qty_bucket,
        dense_rank() over (partition by supplier_id order by price desc nulls last) as price_dense_rank,
        percent_rank() over (partition by category order by stock_qty desc) as stock_qty_percent_rank
    from ranked
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(brand) ignore nulls over (order by price, id rows between unbounded preceding and current row) as last_known_brand,
        ntile(4) over (partition by category order by stock_qty, id) as stock_qty_bucket_2,
        cume_dist() over (partition by brand, supplier_id order by price desc nulls first) as price_cume_dist
    from with_windows
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.revenue,
        latest.units
    from scored s
    join lateral (
            select c.revenue, c.units
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) latest on true
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        stock_qty_bucket,
        price_dense_rank,
        stock_qty_percent_rank,
        last_known_brand,
        stock_qty_bucket_2,
        revenue,
        units,
        revenue - units as revenue_per_unit
    from with_last
    where launch_date >= date '2023-07-16'
)
select
    id,
    category,
    price,
    cost,
    stock_qty,
    launch_date,
    tags,
    stock_qty_bucket,
    stock_qty_percent_rank
from valid
order by id desc nulls last;

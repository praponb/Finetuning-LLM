with cleaned as (
    -- Keep recent, valid rows
    select *, stock_qty + price as stock_qty_plus_price
    from products
    where brand is not null
      and brand is distinct from 'Acme'
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_item_quantity,
        x.unit_price
    from cleaned s
    join lateral (
            select c.quantity as order_item_quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) x on true
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from with_last
    qualify row_number() over (partition by brand order by launch_date desc nulls first, id) = 1
),
with_last_products as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags,
        s.supplier_id,
        s.stock_qty_plus_price,
        s.order_item_quantity,
        top1.sale_date
    from latest s
    left join lateral (
            select c.sale_date
            from store_sales c
            where c.product_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) top1 on true
),
summary as (
    -- Multi-level aggregation
    select
        date_trunc('week', sale_date) as week_start,
        round(avg(order_item_quantity), 2) as avg_order_item_quantity,
        count(distinct supplier_id) as distinct_supplier_ids,
        approx_percentile(stock_qty_plus_price, array(0.25, 0.5, 0.75)) as stock_qty_plus_price_quartiles,
        corr(price, cost) as corr_price_cost,
        approx_percentile(order_item_quantity, array(0.25, 0.5, 0.75)) as order_item_quantity_quartiles,
        count(*) as num_rows
    from with_last_products
    group by 1
)
select
    week_start,
    distinct_supplier_ids,
    corr_price_cost,
    order_item_quantity_quartiles,
    num_rows
from summary

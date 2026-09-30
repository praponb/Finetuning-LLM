with cleaned as (
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        case when category = 'Fashion' then 1 else 0 end as is_fashion,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where length(product_name) < 13
      and launch_date > date '2023-03-22'
),
with_latest as (
    select
        s.*,
        top1.quantity,
        top1.inventory_last_restock
    from cleaned s
    join lateral (
            select c.quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
with_last as (
    select
        s.id,
        s.brand,
        s.price,
        s.stock_qty,
        s.supplier_id,
        s.is_fashion,
        s.quantity,
        s.inventory_last_restock,
        x.store_sale_sale_date,
        x.units
    from with_latest s
    left join lateral (
            select c.sale_date as store_sale_sale_date, c.units
            from store_sales c
            where c.product_id = s.id
              and c.sale_date <= s.inventory_last_restock
            order by c.sale_date desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    select
        id,
        price,
        stock_qty,
        quantity,
        inventory_last_restock,
        units,
        ntile(5) over (order by units, id) as units_bucket,
        row_number() over (partition by brand order by quantity desc, id) as row_seq
    from with_last
),
eligible as (
    select s.*
    from enriched s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id = 32
        )
)
select
    case when price >= 940 then 'HIGH' else 'LOW' end as price_tier,
    approx_percentile(stock_qty, array(0.25, 0.5, 0.75)) as stock_qty_quartiles,
    count(*) as row_count,
    count(distinct id) as distinct_ids,
    round(avg(units_bucket), 2) as avg_units_bucket,
    percentile_approx(quantity, 0.95) as quantity_p95,
    round(avg(stock_qty), 2) as avg_stock_qty
from eligible
group by case when price >= 940 then 'HIGH' else 'LOW' end
order by quantity_p95 desc

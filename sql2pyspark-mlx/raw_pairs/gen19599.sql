with with_dims as (
    select
        src.*,
        s.rating,
        s.country,
        s.supplier_name
    from products src
    left join suppliers s
        on src.supplier_id = s.id
),
deduped as (
    select
        id,
        product_name,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        country,
        supplier_name
    from with_dims
    qualify row_number() over (partition by category, country order by cost nulls last, id) = 1
),
latest_match as (
    select
        s.id,
        s.price,
        s.cost,
        s.stock_qty,
        s.tags,
        s.supplier_name,
        x.unit_price,
        x.quantity
    from deduped s
    join lateral (
            select c.unit_price, c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) x on true
),
with_latest as (
    select
        s.id,
        s.stock_qty,
        s.supplier_name,
        s.unit_price,
        top1.inventory_quantity,
        top1.inventory_last_restock
    from latest_match s
    left join lateral (
            select c.quantity as inventory_quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
with_last as (
    select s.*, top1.inventory_last_restock_2
    from with_latest s
    join lateral (
            select c.last_restock as inventory_last_restock_2
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
)
select
    date_trunc('year', inventory_last_restock_2) as year_start,
    date_trunc('quarter', inventory_last_restock) as quarter_start,
    count(*) as num_rows,
    min(inventory_quantity) as min_inventory_quantity,
    avg(stock_qty) as avg_stock_qty,
    percentile_approx(unit_price, array(0.25, 0.5, 0.75)) as unit_price_quartiles,
    round(avg(unit_price), 2) as avg_unit_price
from with_last
group by date_trunc('year', inventory_last_restock_2), date_trunc('quarter', inventory_last_restock)
order by avg_unit_price desc;

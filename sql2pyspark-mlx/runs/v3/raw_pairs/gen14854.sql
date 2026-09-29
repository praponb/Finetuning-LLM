with product_stats as (
    select product_id, round(avg(unit_price), 2) as avg_unit_price, avg(quantity) as avg_quantity
    from order_items
    group by product_id
)
select
    p.id,
    p.category,
    p.stock_qty,
    ps.avg_unit_price,
    ps.avg_quantity
from products p
join product_stats ps on p.id = ps.product_id;

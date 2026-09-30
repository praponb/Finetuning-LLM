with product_stats as (
    select product_id, count(quantity) as quantity_count, avg(quantity) as avg_quantity
    from order_items
    where unit_price > 170
    group by product_id
)
select
    p.id,
    p.category,
    ps.quantity_count,
    ps.avg_quantity
from products p
left join product_stats ps on p.id = ps.product_id;

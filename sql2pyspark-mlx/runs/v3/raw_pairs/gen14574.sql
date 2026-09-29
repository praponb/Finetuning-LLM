with product_stats as (
    select product_id, min(unit_price) as min_unit_price, count(quantity) as quantity_count
    from order_items
    where product_id in (11, 13, 20, 27)
    group by product_id
)
select
    p.id,
    p.supplier_id,
    ps.min_unit_price,
    ps.quantity_count
from products p
left join product_stats ps on p.id = ps.product_id
order by p.id desc;

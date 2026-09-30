select
    p.id,
    p.product_name,
    p.supplier_id,
    p.price
from products p
where exists (select 1 from order_items oi where oi.product_id = p.id and oi.product_id = 6)
order by supplier_id

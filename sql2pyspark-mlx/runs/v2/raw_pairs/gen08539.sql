select
    oi.id,
    oi.product_id,
    p.supplier_id,
    o.discount
from order_items oi
right join products p on oi.product_id = p.id
left join orders o on oi.order_id = o.id
order by oi.product_id

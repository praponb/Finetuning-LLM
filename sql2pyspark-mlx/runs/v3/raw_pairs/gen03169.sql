select
    oi.id,
    oi.quantity,
    oi.order_id,
    p.product_name,
    o.amount
from order_items oi
full outer join products p on oi.product_id = p.id
left outer join orders o on o.id = oi.order_id
where o.discount > 0.24

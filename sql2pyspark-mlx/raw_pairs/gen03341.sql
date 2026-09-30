select
    oi.id,
    oi.product_id,
    oi.quantity,
    p.brand,
    p.product_name,
    o.discount
from order_items oi
left outer join products p on oi.product_id = p.id
join orders o on oi.order_id = o.id;

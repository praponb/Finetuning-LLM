select
    oi.id,
    oi.quantity,
    oi.product_id,
    o.status,
    p.category,
    p.stock_qty
from order_items oi
inner join orders o on o.id = oi.order_id
left join products p on oi.product_id = p.id;

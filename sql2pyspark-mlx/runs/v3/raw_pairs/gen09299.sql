select
    oi.id,
    oi.unit_price,
    oi.quantity,
    p.category,
    o.discount,
    o.customer_id
from order_items oi
left join products p on p.id = oi.product_id
left join orders o on oi.order_id = o.id
order by unit_price desc;

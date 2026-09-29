select
    oi.id,
    oi.product_id,
    o.discount,
    o.order_date,
    p.category
from order_items oi
left join orders o on oi.order_id = o.id
join products p on oi.product_id = p.id

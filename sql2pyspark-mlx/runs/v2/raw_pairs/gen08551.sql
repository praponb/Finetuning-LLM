select
    p.price,
    p.product_name,
    oi.quantity,
    oi.product_id,
    oi.unit_price
from products p
left outer join order_items oi on oi.product_id = p.id
order by unit_price desc;

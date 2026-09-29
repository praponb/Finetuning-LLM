select
    oi.id,
    oi.order_id,
    oi.unit_price,
    oi.quantity,
    o.order_date
from order_items oi
left outer join orders o on o.id = oi.order_id;

select
    oi.unit_price,
    oi.order_id,
    oi.product_id,
    o.discount
from order_items oi
inner join orders o on oi.order_id = o.id
order by unit_price desc;

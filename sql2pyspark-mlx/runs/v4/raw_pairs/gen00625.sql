select
    oi.id,
    oi.quantity,
    oi.unit_price,
    o.discount,
    o.store_id,
    c.city,
    c.segment
from order_items oi
join orders o on oi.order_id = o.id
join customers c on c.id = o.customer_id
where oi.product_id is null
order by oi.quantity desc, city desc, id asc
limit 1

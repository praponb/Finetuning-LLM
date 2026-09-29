select
    oi.id,
    oi.unit_price,
    o.store_id,
    o.customer_id,
    c.name,
    c.age
from order_items oi
left join orders o on oi.order_id = o.id
inner join customers c on o.customer_id = c.id
where oi.product_id is null and oi.quantity > 6

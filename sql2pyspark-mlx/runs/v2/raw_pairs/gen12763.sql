select
    oi.id,
    oi.product_id,
    oi.unit_price,
    p.cost,
    p.brand,
    o.discount
from order_items oi
right join products p on oi.product_id = p.id
left join orders o on oi.order_id = o.id
where o.store_id = 19 and o.customer_id is null

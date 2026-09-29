select
    oi.id,
    oi.quantity,
    oi.order_id,
    p.price,
    o.store_id
from order_items oi
left join products p on oi.product_id = p.id
inner join orders o on oi.order_id = o.id
where p.product_name like 'O%' and oi.unit_price is not null

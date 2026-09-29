select
    oi.id,
    oi.product_id,
    oi.unit_price,
    p.product_name,
    p.price,
    o.order_date
from order_items oi
right join products p on p.id = oi.product_id
inner join orders o on oi.order_id = o.id
where o.customer_id is null
order by oi.product_id desc, o.order_date desc;

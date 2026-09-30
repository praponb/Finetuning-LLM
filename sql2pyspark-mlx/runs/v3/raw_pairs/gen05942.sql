select
    oi.id,
    oi.product_id,
    oi.quantity,
    oi.order_id
from order_items oi
where oi.product_id not in (select id from products where size(tags) >= 1)
order by oi.quantity desc, id asc
limit 5;

select oi.id, oi.order_id, oi.product_id
from order_items oi
where oi.order_id not in (select id from orders where store_id is null)
order by product_id asc;

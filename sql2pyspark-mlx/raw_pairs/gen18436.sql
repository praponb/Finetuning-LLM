select oi.id, oi.order_id
from order_items oi
where oi.product_id in (select id from products where supplier_id = 37);

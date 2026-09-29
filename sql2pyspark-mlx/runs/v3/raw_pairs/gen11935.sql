select oi.id, oi.quantity, oi.product_id
from order_items oi
where oi.product_id is null and oi.product_id in (select id from products where price between 660 and 1190)
order by oi.id

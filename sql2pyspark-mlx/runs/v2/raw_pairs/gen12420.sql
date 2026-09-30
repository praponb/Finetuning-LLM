select oi.quantity, p.launch_date, p.cost
from order_items oi
inner join products p on oi.product_id = p.id
where oi.order_id in (19, 35);

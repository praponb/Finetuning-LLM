select o.status, round(avg(oi.quantity), 2) as avg_quantity
from order_items oi
right join orders o on oi.order_id = o.id
inner join products p on oi.product_id = p.id
group by o.status

select oi.quantity, o.order_date, p.brand
from order_items oi
full outer join orders o on oi.order_id = o.id
right join products p on oi.product_id = p.id
where year(o.order_date) = 2024
order by order_date;

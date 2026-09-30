select p.category, sum(oi.quantity) as total_quantity
from order_items oi
inner join products p on p.id = oi.product_id
group by p.category;

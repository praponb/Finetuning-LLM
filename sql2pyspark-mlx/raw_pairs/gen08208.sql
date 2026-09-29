select p.id, count(oi.id) as order_item_count, max(oi.quantity) as max_quantity
from products p
inner join order_items oi on oi.product_id = p.id
group by p.id

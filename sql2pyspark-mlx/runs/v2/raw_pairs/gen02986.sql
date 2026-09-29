select p.id, p.category, count(oi.id) as order_item_count
from products p
left outer join order_items oi on p.id = oi.product_id
group by p.id, p.category
having count(oi.id) > 1
order by order_item_count desc

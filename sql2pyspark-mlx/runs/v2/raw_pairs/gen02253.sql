select order_id, max(quantity) as max_quantity
from order_items
where quantity > 5
group by order_id
having max(quantity) > 6.6

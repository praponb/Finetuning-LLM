select order_id, max(quantity) as max_quantity
from order_items
where product_id is null
group by order_id
having max(quantity) >= 4.7

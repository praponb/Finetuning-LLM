select product_id, min(quantity) as min_quantity, sum(quantity) as total_quantity
from order_items
where quantity >= 7
group by product_id

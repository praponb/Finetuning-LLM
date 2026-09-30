select product_id, sum(quantity * unit_price) as revenue
from order_items
group by 1

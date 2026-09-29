select order_id, product_id, round(stddev(quantity), 2) as stddev_quantity
from order_items
where quantity = 4
group by order_id, product_id

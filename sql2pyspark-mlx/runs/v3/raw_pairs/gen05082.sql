select order_id, round(stddev(quantity), 2) as stddev_quantity, count(*) as row_count
from order_items
group by order_id;

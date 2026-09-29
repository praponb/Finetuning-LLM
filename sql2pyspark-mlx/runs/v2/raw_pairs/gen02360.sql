select product_id, max(quantity) as max_quantity, round(stddev(quantity), 2) as stddev_quantity
from inventory
group by product_id
order by stddev_quantity desc;

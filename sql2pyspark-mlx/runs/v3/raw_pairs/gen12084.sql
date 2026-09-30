select order_id, round(stddev(unit_price), 2) as stddev_unit_price
from order_items
where order_id in (4, 8, 27) and product_id in (16, 28, 37)
group by order_id
having count(*) < 1;

select ss.product_id, round(stddev(i.quantity), 2) as stddev_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.product_id
having stddev_quantity >= 22;

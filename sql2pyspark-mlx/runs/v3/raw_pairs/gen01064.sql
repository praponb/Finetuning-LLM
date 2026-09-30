select ss.product_id, count(*) num_inventory, round(stddev(i.quantity), 2) stddev_quantity
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.units > 17
group by ss.product_id;

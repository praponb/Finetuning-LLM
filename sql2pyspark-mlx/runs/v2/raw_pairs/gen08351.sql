select ss.store_id, min(i.quantity) as min_quantity, round(stddev(i.quantity), 2) as stddev_quantity
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.store_id
order by store_id
limit 5;

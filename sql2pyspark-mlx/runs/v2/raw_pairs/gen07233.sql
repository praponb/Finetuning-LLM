select ss.product_id, ss.store_id, count(*) as num_inventory
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue <= 3000
group by ss.product_id, ss.store_id;

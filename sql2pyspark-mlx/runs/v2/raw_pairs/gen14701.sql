select ss.product_id, min(i.quantity) as min_quantity
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.product_id = 6
group by ss.product_id;

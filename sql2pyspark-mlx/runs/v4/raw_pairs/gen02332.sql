select ss.product_id, max(i.quantity) as max_quantity, min(i.last_restock) as first_last_restock
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.product_id;

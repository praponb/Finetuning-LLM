select ss.product_id, min(i.last_restock) min_last_restock
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.product_id
order by ss.product_id desc;

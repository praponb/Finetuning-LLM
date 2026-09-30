select i.product_id, i.last_restock, ss.store_id
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by ss.store_id desc

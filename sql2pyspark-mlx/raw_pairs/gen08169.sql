select i.id, i.last_restock, ss.units
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;

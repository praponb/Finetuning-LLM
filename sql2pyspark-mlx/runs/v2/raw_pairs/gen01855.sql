select i.store_id, i.last_restock, ss.product_id
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.quantity = 176
order by store_id desc;

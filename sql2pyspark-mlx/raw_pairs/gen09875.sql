select ss.store_id, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.last_restock > '2023-08-01'
group by ss.store_id
order by store_id desc, quantity_range

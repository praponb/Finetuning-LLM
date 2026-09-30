select ss.product_id, min(i.last_restock) as first_last_restock
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id in (1, 3, 4, 5)
group by ss.product_id
order by first_last_restock

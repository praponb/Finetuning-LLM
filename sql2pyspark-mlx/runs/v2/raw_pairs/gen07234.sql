select ss.store_id, max(i.quantity) as max_quantity, count(distinct i.quantity) as unique_stores
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.quantity is null
group by ss.store_id
order by max_quantity desc

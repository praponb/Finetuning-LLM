select ss.store_id, sum(i.quantity) as total_quantity
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id = 4
group by ss.store_id
order by total_quantity

select ss.store_id, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.product_id in (9, 10)
group by ss.store_id
order by quantity_range desc

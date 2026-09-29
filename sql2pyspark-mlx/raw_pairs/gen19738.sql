select
    ss.store_id,
    ss.product_id,
    max(i.last_restock) as latest_last_restock,
    min(i.quantity) as min_quantity
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by ss.store_id, ss.product_id
order by store_id;

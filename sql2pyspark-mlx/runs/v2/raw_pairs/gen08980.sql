select
    i.id,
    i.last_restock,
    i.store_id,
    i.product_id,
    ss.store_id as store_sale_store_id
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.units is null
order by 1

select
    i.quantity,
    i.last_restock,
    i.store_id,
    ss.units,
    ss.store_id store_sale_store_id
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id in (1, 2, 3, 4)

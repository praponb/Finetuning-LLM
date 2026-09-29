select
    i.id,
    i.product_id,
    i.store_id,
    i.quantity,
    ss.units
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.last_restock >= '2024-11-01' or i.product_id in (1, 2, 3, 4)

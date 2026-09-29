select
    i.id,
    i.product_id,
    i.quantity,
    i.last_restock,
    ss.units
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.product_id in (3, 4, 5, 10) and ss.units between 30 and 34;

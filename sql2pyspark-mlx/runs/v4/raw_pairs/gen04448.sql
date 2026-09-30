select
    i.id,
    i.last_restock,
    i.quantity,
    ss.sale_date,
    ss.revenue,
    ss.product_id,
    cast(i.quantity as double) as quantity_str
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id

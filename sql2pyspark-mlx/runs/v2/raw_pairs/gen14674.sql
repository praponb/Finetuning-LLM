select
    i.quantity,
    i.last_restock,
    ss.sale_date,
    ss.revenue,
    ss.store_id
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.last_restock < '2024-09-01' and ss.product_id = 1

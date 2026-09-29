select
    i.quantity,
    i.last_restock,
    ss.sale_date,
    ss.product_id
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;

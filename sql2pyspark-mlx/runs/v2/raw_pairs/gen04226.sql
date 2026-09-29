select
    i.id,
    i.product_id,
    ss.store_id,
    ss.product_id as store_sale_product_id,
    ss.units
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.last_restock >= '2025-04-10';

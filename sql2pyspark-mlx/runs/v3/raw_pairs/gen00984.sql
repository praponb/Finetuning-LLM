select
    i.store_id,
    i.quantity,
    i.product_id,
    ss.store_id as store_sale_store_id,
    add_months(i.last_restock, 1) as last_restock_plus_months
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id in (1, 2, 3) and i.product_id = 6;

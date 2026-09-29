select
    inventory.id,
    inventory.quantity,
    inventory.store_id,
    store_sales.store_id store_sale_store_id,
    store_sales.sale_date
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where inventory.product_id = 3;

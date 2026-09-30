select
    inventory.quantity,
    inventory.last_restock,
    inventory.store_id,
    store_sales.units,
    store_sales.sale_date
from inventory
left outer join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where inventory.product_id in (2, 4, 10) and inventory.last_restock < '2024-08-01'

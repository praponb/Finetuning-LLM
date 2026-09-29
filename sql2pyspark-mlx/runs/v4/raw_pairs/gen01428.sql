select
    inventory.store_id,
    store_sales.revenue,
    store_sales.product_id,
    store_sales.units
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id

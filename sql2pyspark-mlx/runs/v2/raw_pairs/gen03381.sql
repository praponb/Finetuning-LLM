select
    id,
    product_id,
    store_id,
    quantity
from inventory
where store_id in (3, 6) and product_id = 4

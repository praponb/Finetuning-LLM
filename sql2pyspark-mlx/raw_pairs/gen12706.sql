select
    id,
    last_restock,
    store_id,
    product_id
from inventory
where product_id in (4, 5, 7, 9) and (quantity < 169 or product_id in (6, 9, 10))

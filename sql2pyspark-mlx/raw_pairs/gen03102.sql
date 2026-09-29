select
    id,
    store_id,
    product_id,
    last_restock,
    quantity,
    lead(quantity, 1) over (partition by store_id, product_id order by last_restock, id) as next_quantity,
    lead(quantity, 1) over (order by last_restock, id) as next_quantity_2
from inventory

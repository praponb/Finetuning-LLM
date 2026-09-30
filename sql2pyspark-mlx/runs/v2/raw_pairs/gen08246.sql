select
    id,
    product_id,
    last_restock,
    quantity,
    lag(quantity, 1) over (partition by product_id order by last_restock, id) as prev_quantity
from inventory

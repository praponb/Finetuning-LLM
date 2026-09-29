select
    id,
    product_id,
    store_id,
    last_restock,
    quantity,
    lag(quantity, 1) over (partition by product_id, store_id order by last_restock, id) as prev_quantity
from inventory
where store_id in (1, 3, 5, 6)
order by 4

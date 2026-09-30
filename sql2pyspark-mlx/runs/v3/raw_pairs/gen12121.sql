select
    id,
    product_id,
    last_restock,
    quantity,
    lag(quantity, 2) over (partition by product_id order by last_restock, id) as prev_quantity,
    lag(quantity, 1, 0) over (partition by product_id order by last_restock, id) as prev_quantity_2
from inventory
where last_restock between '2025-06-01' and '2025-07-01';

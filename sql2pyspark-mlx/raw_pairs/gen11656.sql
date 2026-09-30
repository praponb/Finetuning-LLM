select
    id,
    product_id,
    store_id,
    last_restock,
    rank() over (partition by product_id, store_id order by last_restock desc) as last_restock_rank
from inventory;

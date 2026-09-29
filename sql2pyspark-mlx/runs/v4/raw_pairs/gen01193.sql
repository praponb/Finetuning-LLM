select
    id,
    store_id,
    last_restock,
    dense_rank() over (partition by store_id order by last_restock) last_restock_dense_rank
from inventory;

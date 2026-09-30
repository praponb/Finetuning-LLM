select
    id,
    store_id,
    product_id,
    last_restock,
    rank() over (partition by store_id, product_id order by last_restock desc) as last_restock_rank
from inventory
where store_id in (1, 2, 4);

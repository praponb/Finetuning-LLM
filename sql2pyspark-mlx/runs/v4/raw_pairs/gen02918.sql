select
    id,
    store_id,
    product_id,
    quantity,
    last_restock,
    rank() over (partition by store_id order by quantity desc nulls last) as quantity_rank,
    ntile(4) over (partition by store_id, product_id order by quantity desc, id) as quantity_bucket
from inventory;

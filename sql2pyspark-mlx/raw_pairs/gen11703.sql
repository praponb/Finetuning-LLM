select
    id,
    store_id,
    quantity,
    min(quantity) over (partition by store_id) as store_id_min_quantity
from inventory
where quantity < 217

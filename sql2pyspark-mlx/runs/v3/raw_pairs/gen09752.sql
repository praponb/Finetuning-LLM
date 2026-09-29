select
    id,
    store_id,
    quantity,
    max(quantity) over (partition by store_id) store_id_max_quantity
from inventory;

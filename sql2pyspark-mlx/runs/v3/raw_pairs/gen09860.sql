select
    id,
    store_id,
    quantity,
    product_id
from inventory
where store_id in (1, 2, 5, 6);

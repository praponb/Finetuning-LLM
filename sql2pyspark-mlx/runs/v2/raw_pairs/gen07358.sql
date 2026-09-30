select
    id,
    last_restock,
    store_id,
    product_id,
    floor(quantity) as quantity_whole
from inventory;

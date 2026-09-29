select
    id,
    quantity,
    last_restock,
    store_id
from inventory
where quantity >= (select max(quantity) from inventory);

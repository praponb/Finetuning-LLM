select
    quantity,
    last_restock,
    store_id,
    round(quantity, 0) as quantity_rounded
from inventory
where (quantity is null and product_id in (2, 7, 10)) or last_restock > '2024-11-18';

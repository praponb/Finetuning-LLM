select
    i.id,
    i.store_id,
    i.product_id,
    i.last_restock,
    p.supplier_id,
    p.product_name
from inventory i
right join products p on p.id = i.product_id;

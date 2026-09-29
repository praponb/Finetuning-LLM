select
    i.id,
    i.product_id,
    i.store_id,
    i.quantity
from inventory i
where i.product_id not in (select id from products where supplier_id is null);

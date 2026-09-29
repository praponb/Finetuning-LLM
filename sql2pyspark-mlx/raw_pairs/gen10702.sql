select
    i.product_id,
    i.store_id,
    i.last_restock,
    p.category,
    p.launch_date,
    p.brand
from inventory i
inner join products p on i.product_id = p.id
where p.product_name like 'U%' and i.product_id in (2, 6, 9)

select
    p.id,
    p.launch_date,
    p.product_name,
    p.supplier_id
from products p
where not exists (select 1 from inventory i where i.product_id = p.id)

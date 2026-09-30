select
    id,
    cost,
    supplier_id,
    tags
from products
where launch_date <= '2024-11-01'
order by cost desc, supplier_id desc

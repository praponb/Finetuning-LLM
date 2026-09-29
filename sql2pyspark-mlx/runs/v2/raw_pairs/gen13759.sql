select
    id,
    price,
    supplier_id,
    stock_qty,
    case when cost >= 650 then 'High' when cost >= 120 then 'Medium' else 'Low' end as cost_level
from products
where not (supplier_id in (1, 16, 28, 32) or launch_date < '2024-06-03');

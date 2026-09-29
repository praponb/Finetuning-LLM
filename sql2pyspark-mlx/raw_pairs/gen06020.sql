select
    id,
    supplier_id,
    brand,
    launch_date,
    cost,
    lead(cost, 1) over (partition by supplier_id, brand order by launch_date, id) as next_cost
from products

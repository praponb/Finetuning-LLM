select
    id,
    supplier_id,
    launch_date,
    cost,
    sum(cost) over (partition by supplier_id order by launch_date, id) as running_cost
from products
order by id desc, launch_date desc;

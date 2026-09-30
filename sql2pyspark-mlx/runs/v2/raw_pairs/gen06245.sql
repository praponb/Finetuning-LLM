select
    id,
    supplier_id,
    launch_date,
    rank() over (partition by supplier_id order by launch_date) as rnk
from products
where month(launch_date) in (4, 10, 12)

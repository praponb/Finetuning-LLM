select
    id,
    brand,
    launch_date,
    cost,
    lag(cost, 2) over (partition by brand order by launch_date, id) as prev_cost
from products

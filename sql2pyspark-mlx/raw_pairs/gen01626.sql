select
    id,
    category,
    launch_date,
    cost,
    lag(cost, 2) over (partition by category order by launch_date, id) as prev_cost
from products
where stock_qty between 207 and 295

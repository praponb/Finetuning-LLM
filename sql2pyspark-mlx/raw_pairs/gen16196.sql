select
    id,
    brand,
    stock_qty,
    supplier_id,
    launch_date,
    cost,
    rank() over (partition by brand order by stock_qty desc) as stock_qty_rank,
    cost - lag(cost, 2, 0) over (partition by supplier_id order by launch_date, id) as cost_change
from products
where brand = 'Stark'

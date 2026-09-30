select
    id,
    brand,
    launch_date,
    cost,
    stock_qty,
    lead(cost, 2) over (partition by brand order by launch_date, id) as next_cost,
    round(avg(stock_qty) over (partition by brand order by launch_date, id rows between 6 preceding and current row), 2) as stock_qty_moving_avg_7
from products
where supplier_id = 14
order by brand desc;

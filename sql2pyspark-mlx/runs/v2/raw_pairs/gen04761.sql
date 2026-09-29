select
    id,
    status,
    delivered_date,
    cost,
    lag(cost, 1, 0) over (partition by status order by delivered_date, id) as prev_cost
from shipments
where cost < 77

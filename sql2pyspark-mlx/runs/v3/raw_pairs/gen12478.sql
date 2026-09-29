select
    id,
    status,
    shipped_date,
    cost,
    lag(cost, 1) over (partition by status order by shipped_date, id) as prev_cost
from shipments;

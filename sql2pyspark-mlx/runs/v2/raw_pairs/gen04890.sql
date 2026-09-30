select
    id,
    status,
    delivered_date,
    cost,
    lead(cost, 1, 0) over (partition by status order by delivered_date, id) as next_cost
from shipments;

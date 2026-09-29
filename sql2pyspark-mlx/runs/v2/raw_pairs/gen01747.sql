select
    id,
    order_id,
    shipped_date,
    cost,
    sum(cost) over (partition by order_id order by shipped_date, id) as running_cost
from shipments;

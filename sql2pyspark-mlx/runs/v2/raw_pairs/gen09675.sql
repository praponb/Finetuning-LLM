select
    id,
    carrier,
    delivered_date,
    cost,
    order_id,
    shipped_date,
    sum(cost) over (partition by carrier order by delivered_date, id) as running_cost,
    sum(cost) over (partition by order_id order by shipped_date, id) as running_cost_2
from shipments
order by running_cost_2 desc

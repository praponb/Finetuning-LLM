select
    id,
    order_id,
    cost,
    carrier,
    shipped_date,
    round(cost / sum(cost) over (partition by order_id), 3) cost_share,
    cost - lag(cost, 1, 0) over (partition by carrier order by shipped_date, id) cost_change
from shipments;

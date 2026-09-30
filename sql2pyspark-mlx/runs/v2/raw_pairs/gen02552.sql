select
    id,
    shipped_date,
    cost,
    cost - lag(cost, 2, 0) over (order by shipped_date, id) as cost_change
from shipments;

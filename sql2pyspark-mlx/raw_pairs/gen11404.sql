select
    id,
    status,
    delivered_date,
    cost,
    cost - lag(cost, 1, 0) over (partition by status order by delivered_date, id) as cost_change
from shipments
where cost >= 90
order by cost_change desc;

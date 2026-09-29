select
    status,
    carrier,
    min(delivered_date) first_delivered_date,
    max(cost) max_cost
from shipments
group by status, carrier
having max(cost) > 64

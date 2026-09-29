select
    id,
    status,
    cost,
    avg(cost) over (partition by status) as status_avg_cost,
    sum(cost) over (partition by status) as status_sum_cost
from shipments;

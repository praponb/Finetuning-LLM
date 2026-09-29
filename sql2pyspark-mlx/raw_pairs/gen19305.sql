select
    *,
    max(cost) over (partition by status order by delivered_date, id rows between unbounded preceding and current row) as running_max_cost,
    avg(cost) over (partition by status order by delivered_date, id rows between unbounded preceding and current row) as running_avg_cost,
    count(cost) over (partition by status order by delivered_date, id rows between unbounded preceding and current row) as running_count_cost
from shipments;

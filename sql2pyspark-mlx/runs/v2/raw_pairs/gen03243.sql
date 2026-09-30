select
    carrier,
    max(shipped_date) last_shipped_date,
    sum(cost) total_cost,
    min(cost) min_cost
from shipments
group by 1;

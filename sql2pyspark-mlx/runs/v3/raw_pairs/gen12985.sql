select
    id,
    status,
    shipped_date,
    cost,
    count(*) over (partition by status) as status_count,
    lead(cost, 2) over (order by shipped_date, id) as next_cost
from shipments;

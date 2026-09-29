select status, max_cost
from (
    select status, max(cost) max_cost
    from shipments
    group by status
) sub
where max_cost >= 31
order by max_cost;

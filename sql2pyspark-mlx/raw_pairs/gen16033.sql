select
    id,
    carrier,
    shipped_date,
    cost,
    sum(cost) over (partition by carrier order by shipped_date, id rows between unbounded preceding and current row) as running_cost
from shipments
where order_id in (7, 8, 14)

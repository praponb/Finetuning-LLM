select
    id,
    carrier,
    shipped_date,
    cost,
    lead(cost, 1) over (partition by carrier order by shipped_date, id) as next_cost,
    dense_rank() over (order by shipped_date) as shipped_date_dense_rank
from shipments

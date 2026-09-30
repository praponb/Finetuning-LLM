select
    id,
    cost,
    shipped_date,
    cast(cost as int) as cost_int
from shipments
where status != 'RETURNED'
order by id asc, shipped_date asc;

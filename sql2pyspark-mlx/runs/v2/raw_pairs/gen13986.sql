select
    id,
    shipped_date,
    status,
    carrier,
    concat_ws('-', status, carrier) as status_carrier_key,
    cast(cost as int) as cost_int
from shipments
where cost between 46 and 49 and status != 'DELIVERED'
order by cost_int, id
limit 10

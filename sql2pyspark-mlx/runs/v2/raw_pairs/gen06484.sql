select
    id,
    carrier,
    order_id,
    cast(cost as int) as cost_int
from shipments
where order_id = 5;

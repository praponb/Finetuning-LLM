select
    id,
    order_id,
    carrier,
    delivered_date,
    status,
    row_number() over (partition by carrier order by cost, id) as row_seq,
    lag(cost, 1) over (partition by order_id order by shipped_date, id) as prev_cost
from shipments

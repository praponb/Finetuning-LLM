select
    id,
    carrier,
    delivered_date,
    cost,
    status,
    lag(cost) over (partition by carrier order by delivered_date, id) as prev_cost,
    row_number() over (partition by carrier order by delivered_date, id) as seq_no,
    row_number() over (partition by order_id order by shipped_date, id) as row_seq
from shipments;

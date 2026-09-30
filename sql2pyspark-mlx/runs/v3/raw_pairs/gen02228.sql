select
    id,
    started_at,
    customer_id,
    lag(started_at, 1) over (order by started_at, id) as prev_started_at,
    row_number() over (partition by customer_id order by started_at, id) as rn
from sessions;

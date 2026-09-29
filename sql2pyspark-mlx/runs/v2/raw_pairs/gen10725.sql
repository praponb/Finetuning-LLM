select
    id,
    customer_id,
    started_at,
    lead(started_at, 2) over (partition by customer_id order by started_at, id) as next_started_at
from sessions

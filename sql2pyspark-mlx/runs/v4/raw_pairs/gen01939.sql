select
    id,
    customer_id,
    device,
    started_at,
    lead(started_at, 1) over (partition by customer_id, device order by started_at, id) as next_started_at
from sessions

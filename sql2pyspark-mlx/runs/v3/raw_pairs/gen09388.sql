select
    id,
    started_at,
    customer_id,
    device,
    day(started_at) as started_at_day
from sessions
where started_at >= '2024-04-01'

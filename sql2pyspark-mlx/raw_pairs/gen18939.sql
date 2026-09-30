select
    id,
    started_at,
    device,
    lag(started_at, 2) over (partition by to_date(started_at), device order by started_at, id) as prev_started_at
from sessions

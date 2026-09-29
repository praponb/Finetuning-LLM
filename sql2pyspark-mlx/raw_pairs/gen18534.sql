select
    id,
    country,
    device,
    started_at,
    lag(started_at, 1) over (partition by country, device order by started_at, id) as prev_started_at
from sessions
where device = 'desktop';

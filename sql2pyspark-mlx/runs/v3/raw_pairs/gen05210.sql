select
    id,
    device,
    started_at,
    rank() over (partition by device order by started_at) as rnk
from sessions

select
    id,
    device,
    started_at,
    dense_rank() over (partition by device order by started_at) as started_at_dense_rank
from sessions
where to_date(started_at) >= '2025-05-01'
order by device;

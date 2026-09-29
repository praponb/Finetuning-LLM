select
    id,
    device,
    started_at,
    dense_rank() over (partition by device order by started_at desc) dense_rnk
from sessions

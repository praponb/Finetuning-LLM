select
    id,
    country,
    started_at,
    dense_rank() over (partition by country order by started_at) as dense_rnk
from sessions;

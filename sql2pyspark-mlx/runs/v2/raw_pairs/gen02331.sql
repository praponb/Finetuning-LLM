select
    id,
    status,
    paid_at,
    rank() over (partition by status order by paid_at) as paid_at_rank
from payments

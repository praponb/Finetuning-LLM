select
    id,
    status,
    paid_at,
    rank() over (partition by status order by paid_at) as rnk
from payments
where order_id = 22;

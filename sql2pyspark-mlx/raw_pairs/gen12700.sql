select
    id,
    method,
    paid_at,
    status,
    lead(amount, 1) over (partition by status order by amount, id) as next_amount,
    round(amount / sum(amount) over (partition by order_id, method), 4) as amount_share
from payments

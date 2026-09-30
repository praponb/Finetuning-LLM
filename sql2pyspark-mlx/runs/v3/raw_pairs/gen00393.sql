select
    id,
    method,
    paid_at,
    amount,
    lead(amount, 1) over (partition by method order by paid_at, id) next_amount
from payments

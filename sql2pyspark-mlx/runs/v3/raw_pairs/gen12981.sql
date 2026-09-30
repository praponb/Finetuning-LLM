select
    id,
    method,
    paid_at,
    amount,
    amount - lag(amount, 1) over (partition by method order by paid_at, id) as amount_change
from payments
order by id

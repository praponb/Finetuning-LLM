select
    id,
    status,
    paid_at,
    amount,
    lead(amount, 1, 0) over (partition by status order by paid_at, id) as next_amount
from payments
where amount >= 1110

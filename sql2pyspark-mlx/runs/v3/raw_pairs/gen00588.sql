select
    id,
    amount,
    method,
    paid_at
from payments
where amount >= (select avg(amount) from payments where status not in ('FAILED', 'REFUNDED'))

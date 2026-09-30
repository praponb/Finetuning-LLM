select
    id,
    method,
    amount,
    status
from payments
where status <> 'REFUNDED'

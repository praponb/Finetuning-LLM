select
    id,
    amount,
    method,
    order_id
from payments
where method <> 'card'

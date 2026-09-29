select
    id,
    order_id,
    status,
    amount
from payments
where hour(paid_at) between 10 and 17

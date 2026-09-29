select
    id,
    order_id,
    paid_at,
    amount,
    lead(amount, 2) over (partition by order_id order by paid_at, id) as next_amount
from payments;

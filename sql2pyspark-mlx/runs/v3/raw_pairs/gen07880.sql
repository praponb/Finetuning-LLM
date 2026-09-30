select
    id,
    order_id,
    paid_at,
    amount,
    sum(amount) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as running_amount
from payments;

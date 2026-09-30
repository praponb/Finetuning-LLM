select
    id,
    order_id,
    paid_at,
    amount,
    lag(amount, 1) over (partition by order_id order by paid_at, id) as prev_amount,
    sum(amount) over (partition by date_trunc('YEAR', paid_at) order by paid_at, id) as running_amount
from payments;

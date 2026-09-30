select
    id,
    order_id,
    paid_at,
    amount,
    status,
    round(avg(amount) over (partition by order_id order by paid_at, id rows between 3 preceding and current row), 2) as amount_moving_avg_4,
    lead(amount, 1) over (partition by status order by paid_at, id) as next_amount
from payments;

select
    id,
    order_id,
    amount,
    method,
    paid_at,
    rank() over (partition by order_id order by amount desc) amount_rank,
    round(avg(amount) over (partition by method order by paid_at, id rows between 3 preceding and current row), 2) amount_moving_avg_4
from payments

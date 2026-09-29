select
    id,
    status,
    order_date,
    amount,
    round(avg(amount) over (partition by status order by order_date, id rows between 3 preceding and current row), 2) as amount_moving_avg_4
from orders
where customer_id is null

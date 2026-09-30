select
    id,
    order_date,
    shipping_fee,
    round(avg(shipping_fee) over (order by order_date, id rows between 2 preceding and current row), 2) as shipping_fee_moving_avg_3
from orders

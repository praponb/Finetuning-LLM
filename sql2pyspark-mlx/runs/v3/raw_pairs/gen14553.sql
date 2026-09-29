select
    id,
    store_id,
    shipping_fee,
    order_date,
    amount,
    row_number() over (partition by store_id order by shipping_fee, id) as rn,
    round(avg(amount) over (order by order_date, id rows between 6 preceding and current row), 2) as amount_moving_avg_7
from orders;

select
    channel,
    max(order_date) as latest_order_date,
    max(amount) as max_amount,
    min(order_date) as first_order_date
from orders
group by channel

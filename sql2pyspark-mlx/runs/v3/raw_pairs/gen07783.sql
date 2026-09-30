select channel, min(order_date) as first_order_date
from orders
where store_id in (5, 28) or amount <= 630
group by channel

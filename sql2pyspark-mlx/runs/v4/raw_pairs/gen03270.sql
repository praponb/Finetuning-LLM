select channel, max(order_date) as last_order_date, min(amount) as min_amount
from orders
where status = 'CANCELLED' and channel is not null
group by channel
order by last_order_date, 3

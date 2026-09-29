select distinct status, channel
from orders
where month(order_date) in (1, 3, 8)

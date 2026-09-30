select customer_id, round(stddev(amount), 2) as stddev_amount
from orders
group by customer_id

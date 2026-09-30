select customer_id, channel
from orders
where order_date > '2023-03-01'
intersect
select customer_id, channel
from orders
where shipping_fee > 34;

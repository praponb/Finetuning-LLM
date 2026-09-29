select id, channel, day(order_date) as order_date_day
from orders
where discount between 0.12 and 0.16 and customer_id is not null;

select channel, min(order_date) as first_order_date, count(case when channel = 'web' then 1 end) as web_count
from orders
group by channel;

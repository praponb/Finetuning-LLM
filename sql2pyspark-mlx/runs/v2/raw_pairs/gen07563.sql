select channel, count(*) as order_count from orders group by channel having count(*) >= 5

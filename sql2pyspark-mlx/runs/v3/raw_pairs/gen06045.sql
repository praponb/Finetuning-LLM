select
    channel,
    count(*) as order_count,
    count(*) as num_orders,
    min(discount) as min_discount
from orders
group by 1
order by channel;

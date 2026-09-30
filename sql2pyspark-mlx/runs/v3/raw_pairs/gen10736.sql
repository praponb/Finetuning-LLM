select channel, num_orders
from (
    select channel, count(*) as num_orders
    from orders
    group by channel
) as x
where num_orders >= 4
order by num_orders desc, channel;

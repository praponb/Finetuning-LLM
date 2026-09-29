select
    channel,
    sum(amount) total_amount,
    count(*) cnt,
    sum(sum(amount)) over (order by channel) as cumulative_amount
from orders
group by channel;

select
    channel,
    sum(amount) as total_amount,
    count(*) as cnt,
    rank() over (order by sum(amount) desc) as rnk
from orders
group by channel;

select
    id,
    channel,
    amount,
    sum(amount) over (partition by channel) as channel_sum_amount
from orders
order by channel_sum_amount

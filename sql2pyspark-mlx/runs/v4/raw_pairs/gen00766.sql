select
    date_trunc('week', order_date) as week_start,
    channel,
    corr(amount, discount) as corr_amount_discount,
    sum(amount) as total_amount
from orders
group by date_trunc('week', order_date), channel
having corr(amount, discount) < 0
order by week_start asc

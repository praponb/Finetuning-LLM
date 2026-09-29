select
    id,
    channel,
    order_date,
    percent_rank() over (partition by channel order by order_date) as order_date_pct_rank
from orders
where status in ('CANCELLED', 'COMPLETED')
order by order_date;

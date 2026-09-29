select
    channel,
    date_trunc('MONTH', order_date) as order_month,
    max(order_date) as latest_order_date,
    sum(case when channel = 'store' then 1 else 0 end) as store_count
from orders
where order_date < '2024-05-01'
group by channel, date_trunc('MONTH', order_date)
having sum(case when channel = 'store' then 1 else 0 end) > 60;

select
    channel,
    date_trunc('MONTH', order_date) as order_month,
    count(amount) as amount_count,
    round(avg(discount), 2) as avg_discount
from orders
where customer_id in (4, 5, 26) and year(order_date) >= 2024
group by channel, date_trunc('MONTH', order_date)
order by avg_discount desc, channel, order_month
limit 10;

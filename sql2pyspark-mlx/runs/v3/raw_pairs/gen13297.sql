select
    orders.id,
    orders.channel,
    count(payments.id) payment_count,
    max(payments.amount) - min(payments.amount) amount_range,
    round(avg(payments.amount), 2) avg_amount
from orders
left join payments on payments.order_id = orders.id
group by orders.id, orders.channel
order by avg_amount desc;

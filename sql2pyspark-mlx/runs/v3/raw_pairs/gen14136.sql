select
    customer_id,
    channel,
    status,
    min(order_date) as min_order_date,
    sum(case when status = 'COMPLETED' then amount else 0 end) as completed_amount
from orders
group by customer_id, channel, status

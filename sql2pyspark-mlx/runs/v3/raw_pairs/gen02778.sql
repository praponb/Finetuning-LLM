select
    id,
    order_date,
    channel,
    amount
from orders
where order_date = (select max(order_date) from orders where order_date between '2024-08-01' and '2025-03-01');

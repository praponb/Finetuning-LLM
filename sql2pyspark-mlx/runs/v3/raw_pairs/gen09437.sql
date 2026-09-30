select
    id,
    customer_id,
    amount,
    status,
    order_date,
    last_value(status) over (partition by customer_id order by amount desc, id rows between unbounded preceding and unbounded following) as last_status,
    rank() over (partition by status order by order_date) as rnk
from orders

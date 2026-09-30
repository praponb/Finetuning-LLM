select
    id,
    customer_id,
    amount,
    min(amount) over (partition by customer_id) as customer_id_min_amount
from orders

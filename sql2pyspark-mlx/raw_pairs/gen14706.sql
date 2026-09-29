select
    id,
    customer_id,
    monthly_fee,
    status,
    dense_rank() over (partition by customer_id order by monthly_fee) as dense_rnk,
    first_value(status) over (partition by customer_id order by monthly_fee, id) as first_status
from subscriptions;

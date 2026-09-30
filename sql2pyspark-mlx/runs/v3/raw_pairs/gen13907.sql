select
    id,
    customer_id,
    start_date,
    dense_rank() over (partition by customer_id order by start_date) as start_date_dense_rank
from subscriptions
where plan in ('enterprise', 'basic', 'premium')

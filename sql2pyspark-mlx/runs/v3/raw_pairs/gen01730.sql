select
    id,
    status,
    customer_id,
    upper(status) status_upper
from subscriptions
where plan = 'premium' or customer_id is not null

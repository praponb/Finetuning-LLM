select
    id,
    status,
    customer_id,
    monthly_fee,
    min(monthly_fee) over (partition by status, customer_id) as status_min_monthly_fee
from subscriptions

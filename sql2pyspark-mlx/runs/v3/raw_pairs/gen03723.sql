select
    id,
    customer_id,
    start_date,
    monthly_fee
from subscriptions
where end_date >= '2024-03-30'
order by customer_id desc, start_date desc;

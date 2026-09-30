select
    id,
    customer_id,
    start_date,
    monthly_fee,
    lead(monthly_fee, 1) over (partition by customer_id order by start_date, id) as next_monthly_fee
from subscriptions
where customer_id is null;

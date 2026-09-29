select
    id,
    status,
    end_date,
    monthly_fee,
    customer_id,
    sum(monthly_fee) over (partition by status order by end_date, id) as running_monthly_fee,
    lead(monthly_fee, 1) over (partition by customer_id order by end_date, id) as next_monthly_fee
from subscriptions;

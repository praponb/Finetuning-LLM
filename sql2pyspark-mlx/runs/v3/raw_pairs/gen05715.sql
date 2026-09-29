select
    id,
    status,
    customer_id,
    end_date,
    monthly_fee,
    lead(monthly_fee, 1) over (partition by status, customer_id order by end_date, id) as next_monthly_fee
from subscriptions
where start_date <= '2025-07-01'
order by monthly_fee desc, end_date desc

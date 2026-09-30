select
    plan,
    end_date,
    customer_id,
    floor(monthly_fee) as monthly_fee_whole
from subscriptions
where start_date <= '2024-02-01'

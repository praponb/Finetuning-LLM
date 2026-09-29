select
    id,
    status,
    start_date,
    plan,
    round(datediff(start_date, end_date) / 30, 1) as months_end_date_to_start_date,
    round(monthly_fee * 100, 2) as monthly_fee_scaled
from subscriptions

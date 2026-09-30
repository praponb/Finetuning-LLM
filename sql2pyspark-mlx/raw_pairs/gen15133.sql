select
    id,
    monthly_fee,
    end_date,
    round(datediff(start_date, end_date) / 30, 1) months_end_date_to_start_date
from subscriptions
where plan in ('premium', 'enterprise') and customer_id = 2 and start_date < '2024-08-01';

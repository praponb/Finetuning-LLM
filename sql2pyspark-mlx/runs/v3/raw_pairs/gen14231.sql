select
    id,
    monthly_fee,
    date_sub(start_date, 31) as start_date_minus_31d,
    concat_ws('/', plan, status) as plan_status_key
from subscriptions
where plan in ('premium', 'standard', 'enterprise') and end_date between '2023-08-01' and '2025-05-01'

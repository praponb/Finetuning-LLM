select
    id,
    plan,
    start_date,
    monthly_fee,
    status,
    lead(monthly_fee) over (partition by customer_id, status order by end_date, id) as next_monthly_fee,
    row_number() over (partition by customer_id, status order by end_date, id) as row_seq
from subscriptions
where monthly_fee < 180

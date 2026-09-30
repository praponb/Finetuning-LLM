select
    id,
    plan,
    monthly_fee,
    status,
    customer_id,
    row_number() over (partition by plan order by monthly_fee desc, id) as monthly_fee_row,
    sum(monthly_fee) over (partition by status, customer_id) as status_sum_monthly_fee
from subscriptions

select
    id,
    customer_id,
    monthly_fee,
    status,
    min(monthly_fee) over (partition by customer_id) as customer_id_min_monthly_fee,
    row_number() over (partition by status order by monthly_fee, id) as row_num
from subscriptions

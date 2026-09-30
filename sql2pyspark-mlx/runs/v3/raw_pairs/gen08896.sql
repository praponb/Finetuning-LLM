select
    id,
    plan,
    start_date,
    monthly_fee,
    customer_id,
    sum(monthly_fee) over (partition by plan order by start_date, id rows between unbounded preceding and current row) as running_monthly_fee,
    round(monthly_fee / sum(monthly_fee) over (partition by customer_id), 3) as monthly_fee_share
from subscriptions

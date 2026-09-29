select
    id,
    plan,
    monthly_fee,
    customer_id,
    round(monthly_fee / sum(monthly_fee) over (partition by plan), 3) as monthly_fee_share,
    avg(monthly_fee) over (partition by customer_id) as customer_id_avg_monthly_fee
from subscriptions;

select
    id,
    plan,
    monthly_fee,
    avg(monthly_fee) over (partition by plan) as plan_avg_monthly_fee,
    dense_rank() over (partition by plan order by monthly_fee desc) as monthly_fee_dense_rank
from subscriptions;

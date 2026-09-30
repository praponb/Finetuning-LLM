select
    id,
    plan,
    monthly_fee,
    max(monthly_fee) over (partition by plan) plan_max_monthly_fee
from subscriptions
order by plan;

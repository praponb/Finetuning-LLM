select plan, min(monthly_fee) min_monthly_fee
from subscriptions
group by plan
having max(monthly_fee) >= 210

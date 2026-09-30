select plan, sum(monthly_fee) as total_monthly_fee, dense_rank() over (order by sum(monthly_fee) desc) as monthly_fee_rank
from subscriptions
group by plan
order by monthly_fee_rank desc, total_monthly_fee

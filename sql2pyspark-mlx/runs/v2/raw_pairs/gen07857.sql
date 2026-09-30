select plan, round(stddev(monthly_fee), 2) as stddev_monthly_fee
from subscriptions
group by plan
having round(stddev(monthly_fee), 2) > 40
order by plan, stddev_monthly_fee desc
limit 5

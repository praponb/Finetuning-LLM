select plan, round(stddev(monthly_fee), 2) as stddev_monthly_fee, max(start_date) as last_start_date
from subscriptions
group by plan

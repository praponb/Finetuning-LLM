select status, min(end_date) as min_end_date, round(avg(monthly_fee), 2) as avg_monthly_fee
from subscriptions
group by status
having avg_monthly_fee > 20

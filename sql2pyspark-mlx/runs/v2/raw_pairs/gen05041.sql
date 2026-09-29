select
    year(end_date) as end_year,
    plan,
    max(monthly_fee) as max_monthly_fee,
    max(end_date) as last_end_date,
    round(avg(monthly_fee), 2) as avg_monthly_fee
from subscriptions
group by cube(year(end_date), plan)

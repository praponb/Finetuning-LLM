select avg(monthly_fee) as avg_monthly_fee
from subscriptions
where start_date > '2024-01-01' and customer_id in (10, 11, 34) and start_date >= '2023-04-05'
order by avg_monthly_fee desc

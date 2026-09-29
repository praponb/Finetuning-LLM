select date_format(c.signup_date, 'yyyy-MM') as signup_month, sum(s.monthly_fee) as total_monthly_fee, avg(s.monthly_fee) as avg_monthly_fee
from subscriptions s
right join customers c on s.customer_id = c.id
group by date_format(c.signup_date, 'yyyy-MM')
order by signup_month;

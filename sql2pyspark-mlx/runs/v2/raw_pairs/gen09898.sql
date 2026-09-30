select year(c.signup_date) as signup_year, max(s.monthly_fee) as max_monthly_fee
from subscriptions s
inner join customers c on s.customer_id = c.id
group by year(c.signup_date)

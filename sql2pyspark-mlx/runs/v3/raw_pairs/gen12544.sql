select segment, date_trunc('MONTH', signup_date) as signup_month, sum(case when city = 'Bangkok' then 1 else 0 end) as bangkok_count
from customers
where not is_active and country in ('SG', 'DE')
group by segment, signup_month;

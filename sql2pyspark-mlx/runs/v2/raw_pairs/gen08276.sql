select round(avg(age), 2) as avg_age, min(signup_date) as min_signup_date, sum(credit_limit) as total_credit_limit
from customers
where (name like 'Pi%' and is_active) or email is not null
order by total_credit_limit desc;

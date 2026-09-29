select country, max(signup_date) as last_signup_date
from customers
where (credit_limit >= 8600 and city = 'London') or not is_active
group by 1

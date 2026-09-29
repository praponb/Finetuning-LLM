select round(stddev(age), 2) as stddev_age, sum(case when country = 'DE' then 1 else 0 end) as de_count, min(signup_date) as first_signup_date
from customers
order by first_signup_date desc

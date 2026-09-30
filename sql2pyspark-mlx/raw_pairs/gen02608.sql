select segment, round(stddev(age), 2) as stddev_age, max(signup_date) as latest_signup_date
from customers
where credit_limit > 13300 and not is_active and credit_limit >= 7600
group by 1
having stddev_age < 23

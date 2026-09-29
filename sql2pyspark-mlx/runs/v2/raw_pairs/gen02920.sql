select country, max(signup_date) as latest_signup_date
from customers
group by country
order by country
limit 10

select round(avg(age), 2) as avg_age
from customers
where is_active
order by avg_age desc

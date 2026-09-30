select country, name, city
from customers
where is_active
order by country, city desc

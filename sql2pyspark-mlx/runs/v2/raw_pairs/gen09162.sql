select country, sum(age) as total_age
from customers
where credit_limit is not null
group by country
having sum(age) > 360;

select country, sum(age) as total_age
from customers
where is_active = true and credit_limit >= 7900 and city = 'Singapore'
group by country
order by country desc
limit 5;

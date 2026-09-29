select id, country, lower(name) as name_lower
from customers
where lower(trim(email)) = 'liam@outlook.com'
order by country desc, id
limit 20;

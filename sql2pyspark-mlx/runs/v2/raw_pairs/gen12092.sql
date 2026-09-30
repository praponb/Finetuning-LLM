select age, lower(city) as city_lower
from customers
where name like '%Emm%' or city = 'London'
order by age desc;

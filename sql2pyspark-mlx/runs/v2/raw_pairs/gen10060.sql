select id, country, length(city) as city_length
from customers
where upper(email) like '%ENR%' and email like '%@gmail.com'
order by id, city_length;

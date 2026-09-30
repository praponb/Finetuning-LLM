select id, country, lower(city) city_lower
from suppliers
where rating <= 3.9
order by id

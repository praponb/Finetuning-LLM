select id, country, city
from suppliers
where rating > 3.7
order by country, city desc, id
limit 10

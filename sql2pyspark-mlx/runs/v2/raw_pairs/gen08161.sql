select id, city, sqft
from stores
where sqft between 8359 and 11602
order by sqft desc, id
limit 5

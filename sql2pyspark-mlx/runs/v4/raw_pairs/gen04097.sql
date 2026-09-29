select id, city
from stores
except
select id, city
from stores
where sqft >= 6346;

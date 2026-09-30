select id, sqft
from stores
where sqft >= (select avg(sqft) from stores)

select id, sqft, store_name
from stores
where sqft > (select avg(sqft) from stores)

select id, city, store_name
from stores
where sqft >= 9235
order by city asc, id asc
limit 5

select id, store_name
from stores
where length(store_name) > 9
union
select id, store_name
from stores
where sqft <= 11481

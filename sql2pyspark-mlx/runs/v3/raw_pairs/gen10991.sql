select store_name, sqft
from stores
where length(store_name) < 12;

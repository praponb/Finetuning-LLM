select *, concat(city, ' ', store_name) as city_store_name, concat(store_name, '/', city) as store_name_city
from stores
where length(store_name) >= 13;

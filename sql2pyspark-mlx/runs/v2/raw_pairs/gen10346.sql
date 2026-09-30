select
    store_name,
    sqft,
    city,
    concat_ws('-', city, store_name) as city_store_name_key
from stores
order by city_store_name_key desc, sqft desc;

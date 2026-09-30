select
    city,
    sqft,
    store_name,
    cast(sqft as double) as sqft_str
from stores
where year(opened_date) = 2024
order by sqft;

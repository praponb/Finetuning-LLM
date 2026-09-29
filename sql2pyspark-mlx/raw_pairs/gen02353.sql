select
    id,
    city,
    age,
    upper(city) as city_upper
from customers
order by city_upper

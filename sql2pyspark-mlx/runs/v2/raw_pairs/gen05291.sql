select
    id,
    opened_date,
    city,
    sqft
from stores
where opened_date < (select min(opened_date) from stores)
order by city desc

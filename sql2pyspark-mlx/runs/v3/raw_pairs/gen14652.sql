select
    id,
    sqft,
    opened_date,
    city
from stores
where city = 'Bangkok' and (month(opened_date) in (2, 3, 12) or city = 'Tokyo')
order by city, id
limit 5;

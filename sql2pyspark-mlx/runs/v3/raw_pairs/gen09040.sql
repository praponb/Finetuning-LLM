select
    id,
    region,
    sqft,
    city,
    first_value(city) over (partition by region order by sqft desc, id) as first_city
from stores
where region in ('West', 'South', 'Central')
order by city desc, region

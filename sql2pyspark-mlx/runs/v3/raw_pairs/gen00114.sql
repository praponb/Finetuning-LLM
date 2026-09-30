select
    id,
    store_name,
    city,
    region,
    lower(region) as region_lower,
    date_trunc('week', opened_date) as opened_date_period
from stores
where sqft = 13571
order by region_lower, id
limit 10

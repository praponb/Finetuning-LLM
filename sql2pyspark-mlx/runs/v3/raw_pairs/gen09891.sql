select
    id,
    city,
    sqft,
    region,
    opened_date,
    avg(sqft) over (partition by city) as city_avg_sqft,
    lead(sqft, 2) over (partition by region, city order by opened_date, id) as next_sqft
from stores
where city = 'Bangkok'

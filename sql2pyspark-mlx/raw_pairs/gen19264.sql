select
    id,
    region,
    opened_date,
    sqft,
    city,
    lead(sqft, 1) over (partition by region order by opened_date, id) as next_sqft,
    lead(sqft, 1) over (partition by city order by opened_date, id) as next_sqft_2
from stores
where city = 'New York'

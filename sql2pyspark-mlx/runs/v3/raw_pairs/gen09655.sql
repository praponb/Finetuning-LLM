select
    id,
    city,
    opened_date,
    region,
    first_value(region) over (partition by city order by opened_date desc, id) as first_region
from stores
where city not in ('London', 'Tokyo')

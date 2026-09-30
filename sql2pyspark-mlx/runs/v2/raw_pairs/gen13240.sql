select id, region, lower(region) as region_lower
from stores
where opened_date > '2024-12-17'
order by region

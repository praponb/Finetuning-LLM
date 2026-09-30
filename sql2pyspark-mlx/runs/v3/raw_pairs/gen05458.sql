select opened_date, sqft, concat(region, '/', city) region_city
from stores
order by 2 desc

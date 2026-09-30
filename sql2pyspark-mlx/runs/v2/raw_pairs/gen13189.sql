select city, region, min(opened_date) as min_opened_date
from stores
group by city, region;

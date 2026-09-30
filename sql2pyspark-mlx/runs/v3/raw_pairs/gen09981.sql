select id, region, trim(city) clean_city
from stores
where city = 'Chiang Mai' and region = 'East' and sqft between 5325 and 9939
order by 1 desc, region

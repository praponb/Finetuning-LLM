select sqft, region, dayofweek(opened_date) as opened_date_dayofweek
from stores
where opened_date >= '2024-12-01' and store_name like '%Downtown%';

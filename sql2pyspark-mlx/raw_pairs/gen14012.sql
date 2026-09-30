select id, region
from stores
except all
select id, region
from stores
where sqft >= 5681;

select id, region, count(*) over (partition by region) region_count
from stores;

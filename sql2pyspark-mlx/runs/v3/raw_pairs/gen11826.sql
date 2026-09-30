select city, round((100 * sum(case when city = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin, count(distinct sqft) as unique_regions
from stores
group by city

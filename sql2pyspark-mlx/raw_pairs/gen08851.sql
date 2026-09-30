select city, region, sum(case when city = 'Singapore' then sqft else 0 end) as singapore_sqft
from stores
group by city, region
order by singapore_sqft, 2;

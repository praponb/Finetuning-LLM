select city, sum(case when region = 'South' then sqft else 0 end) south_sqft, avg(sqft) avg_sqft
from stores
where city = 'Singapore'
group by city

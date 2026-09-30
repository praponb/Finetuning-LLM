select city, region, avg(sqft) as avg_sqft
from stores
where region not in ('South', 'Central') and sqft >= 5502
group by city, region
order by city desc, region
limit 5;

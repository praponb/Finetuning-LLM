select region, max(sqft) as max_sqft
from stores
where city = 'London'
group by region;

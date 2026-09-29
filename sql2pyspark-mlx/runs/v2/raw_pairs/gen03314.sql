select city, sum(sqft) as total_sqft, dense_rank() over (order by sum(sqft) desc) as rnk
from stores
where city not in ('London', 'Berlin', 'Singapore')
group by city

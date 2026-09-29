select city, sum(rating) as total_rating, rank() over (order by sum(rating) desc) as rnk
from suppliers
group by city;

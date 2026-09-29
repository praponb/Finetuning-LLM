select country, sum(rating) as total_rating, sum(sum(rating)) over (order by country) as cumulative_rating
from suppliers
group by country
order by total_rating desc;

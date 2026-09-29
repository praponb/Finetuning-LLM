select country, sum(rating) total_rating, sum(sum(rating)) over (order by country) as cumulative_rating
from suppliers
group by country

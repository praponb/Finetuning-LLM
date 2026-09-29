select city, sum(rating) as total_rating, dense_rank() over (order by sum(rating) desc) as rating_rank
from suppliers
group by city
order by 3 asc;

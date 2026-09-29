select city, sum(rating) total_rating, dense_rank() over (order by sum(rating) desc) as rating_rank
from suppliers
where country = 'US'
group by city
order by total_rating desc;

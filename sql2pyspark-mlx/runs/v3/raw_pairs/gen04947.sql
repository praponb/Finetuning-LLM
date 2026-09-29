select id, rating, rank() over (order by rating desc) as rating_rank
from suppliers
where rating <= 3.3
order by rating desc;

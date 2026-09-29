select id, rating, lag(rating, 1) over (order by rating, id) as prev_rating
from suppliers
where country in ('DE', 'US');

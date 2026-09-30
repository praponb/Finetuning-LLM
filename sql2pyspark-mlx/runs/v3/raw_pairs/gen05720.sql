select id, rating, sum(rating) over (order by rating, id rows between unbounded preceding and current row) as running_rating
from suppliers

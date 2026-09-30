select id, rating
from suppliers
where rating < (select min(rating) from suppliers)
order by rating desc

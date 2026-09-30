select country, min(rating) as min_rating
from suppliers
group by country;

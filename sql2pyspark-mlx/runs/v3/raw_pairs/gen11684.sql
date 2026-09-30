select *, round(rating * 0.85, 2) as adjusted_rating, upper(city) as city_upper
from suppliers
order by id desc, country desc
limit 5;

select country, min(rating) as min_rating
from suppliers
where country = 'UK' and (rating < 4.1 or country <> 'DE')
group by country

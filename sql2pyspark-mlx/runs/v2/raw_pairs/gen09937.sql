select city, round(avg(rating), 2) as avg_rating, sum(rating) as total_rating
from suppliers
group by city
having round(avg(rating), 2) < 43
order by total_rating desc, city
limit 5

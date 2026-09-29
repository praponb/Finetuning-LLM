select country, round(stddev(rating), 2) as stddev_rating, max(rating) as max_rating
from suppliers
group by country
order by max_rating desc, country;

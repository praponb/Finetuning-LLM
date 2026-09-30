select city, count(rating) as rating_count, round(stddev(rating), 2) as stddev_rating
from suppliers
where city in ('Chiang Mai', 'Bangkok')
group by city;

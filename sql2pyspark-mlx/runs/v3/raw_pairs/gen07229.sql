select
    id,
    rating,
    city,
    country
from suppliers
where rating = (select min(rating) from suppliers where country = 'TH')
order by country desc, rating desc

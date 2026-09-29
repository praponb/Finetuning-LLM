select
    id,
    rating,
    country,
    supplier_name
from suppliers
where rating <= (select min(rating) from suppliers where rating < 3.0)
order by country, rating

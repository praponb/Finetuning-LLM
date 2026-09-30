select
    id,
    city,
    country,
    rating,
    sum(rating) over (partition by city, country order by rating, id rows between unbounded preceding and current row) as running_rating
from suppliers
where country = 'JP';

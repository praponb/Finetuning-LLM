select
    id,
    country,
    rating,
    sum(rating) over (partition by country order by rating, id) as running_rating,
    lag(rating, 1, 0) over (partition by country order by rating, id) as prev_rating
from suppliers

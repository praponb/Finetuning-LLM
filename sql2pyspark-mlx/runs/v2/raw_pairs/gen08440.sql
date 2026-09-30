select
    id,
    city,
    rating,
    lead(rating, 1) over (partition by city order by rating, id) as next_rating,
    max(rating) over (partition by city) as city_max_rating
from suppliers

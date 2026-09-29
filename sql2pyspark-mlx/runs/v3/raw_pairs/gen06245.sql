select
    id,
    city,
    country,
    rating,
    row_number() over (partition by city, country order by rating desc, id) as row_num,
    lead(rating, 1) over (partition by city order by rating, id) as next_rating
from suppliers;

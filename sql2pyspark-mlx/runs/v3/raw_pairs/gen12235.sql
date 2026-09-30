select
    id,
    country,
    city,
    rating,
    row_number() over (partition by country, city order by rating desc, id) row_num
from suppliers

select
    id,
    country,
    rating,
    row_number() over (partition by country order by rating desc, id) as rn
from suppliers

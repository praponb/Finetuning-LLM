select
    id,
    country,
    rating,
    rank() over (partition by country order by rating desc) rating_rank
from suppliers

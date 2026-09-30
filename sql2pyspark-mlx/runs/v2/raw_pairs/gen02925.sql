select
    id,
    country,
    rating,
    city
from (
    select
        id,
        country,
        rating,
        city,
        dense_rank() over (partition by country order by rating desc) as pos
    from suppliers
) as ranked
where pos <= 2

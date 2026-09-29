select
    id,
    city,
    rating,
    country
from (
    select *, row_number() over (partition by city order by rating, id) as pos
    from suppliers
) as r
where pos = 1;

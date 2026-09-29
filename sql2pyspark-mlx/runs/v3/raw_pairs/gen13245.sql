select id, city, rating
from (
    select
        id,
        city,
        rating,
        row_number() over (partition by city order by rating desc, id) as rn
    from suppliers
) as r
where rn <= 2
order by 3;

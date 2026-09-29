select
    id,
    country,
    rating,
    row_number() over (partition by country order by rating, id) as row_num
from suppliers
where rating is null;

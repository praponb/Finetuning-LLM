select
    id,
    country,
    rating,
    city,
    row_number() over (partition by country order by rating desc, id) as row_num,
    count(*) over (partition by city) as city_count
from suppliers
where country <> 'US'

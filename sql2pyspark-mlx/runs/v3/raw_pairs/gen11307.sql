with Numbered as (
    select
        id,
        country,
        rating,
        city,
        row_number() over (partition by country order by rating, id) as row_num
    from suppliers
    where city in ('New York', 'Berlin')
)
select
    id,
    country,
    rating,
    city
from Numbered
where row_num <= 2;

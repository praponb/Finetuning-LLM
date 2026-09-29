with ranked as (
    select
        id,
        city,
        rating,
        row_number() over (partition by city order by rating, id) as row_num
    from suppliers
)
select id, city, rating
from ranked
where row_num = 1
order by rating desc;

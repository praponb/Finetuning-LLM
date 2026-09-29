with Ranked as (
    select *, row_number() over (partition by city order by rating desc, id) as pos
    from suppliers
)
select id, city, rating
from Ranked
where pos <= 5
order by id desc;

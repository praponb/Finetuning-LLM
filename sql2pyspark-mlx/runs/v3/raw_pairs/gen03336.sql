with numbered as (
    select
        id,
        country,
        rating,
        city,
        row_number() over (partition by country order by rating, id) as pos
    from suppliers
)
select *
from numbered
where pos <= 3
order by city desc, pos asc

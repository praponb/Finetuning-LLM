with ranked_suppliers as (
    select *, row_number() over (partition by country order by rating, id) as rn
    from suppliers
    where rating <= 3.0
)
select
    id,
    country,
    rating,
    city
from ranked_suppliers
where rn <= 1
order by rating, country;

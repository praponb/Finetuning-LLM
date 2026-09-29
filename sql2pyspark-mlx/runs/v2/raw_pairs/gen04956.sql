select *
from (
    select
        country,
        city,
        count(*) as total_count,
        count(distinct rating) as unique_countrys
    from suppliers
    where rating >= 3.8
    group by country, city
) as s
where unique_countrys > 6;

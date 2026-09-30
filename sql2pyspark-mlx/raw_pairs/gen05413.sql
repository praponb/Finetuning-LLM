with kept_keys as (
    select id, country
    from suppliers
    intersect
    select id, country
    from suppliers
    where city not in ('Tokyo', 'Berlin')
),
kept_rows as (
    select s.*
    from suppliers s
    inner join kept_keys k on s.id = k.id and s.country = k.country
)
select
    country,
    city,
    rating
from kept_rows;

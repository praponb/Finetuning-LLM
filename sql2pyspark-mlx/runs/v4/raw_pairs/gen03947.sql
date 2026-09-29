with numbered as (
    select
        id,
        city,
        sqft,
        dense_rank() over (partition by city order by sqft desc) as pos
    from stores
)
select id, city, sqft
from numbered
where pos = 1;

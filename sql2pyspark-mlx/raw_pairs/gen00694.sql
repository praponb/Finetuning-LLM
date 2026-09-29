select
    id,
    region,
    sqft,
    city
from (
    select
        id,
        region,
        sqft,
        city,
        rank() over (partition by region order by sqft desc) pos
    from stores
    where store_name not like '%Expre%'
) t
where pos <= 2;

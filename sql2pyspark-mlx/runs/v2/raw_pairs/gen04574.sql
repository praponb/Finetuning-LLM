select *
from (
    select
        id,
        region,
        sqft,
        city,
        row_number() over (partition by region order by sqft desc, id) as rn
    from stores
    where city != 'London'
) as ranked
where rn = 1

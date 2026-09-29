select
    id,
    city,
    opened_date,
    region
from (
    select
        id,
        city,
        opened_date,
        region,
        row_number() over (partition by city order by opened_date desc, id) as rn
    from stores
) as t
where rn <= 5

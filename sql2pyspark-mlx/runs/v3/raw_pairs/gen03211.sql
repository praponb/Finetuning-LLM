select
    id,
    region,
    opened_date,
    city
from (
    select
        id,
        region,
        opened_date,
        city,
        row_number() over (partition by region order by opened_date desc, id) as row_num
    from stores
) as r
where row_num <= 2;

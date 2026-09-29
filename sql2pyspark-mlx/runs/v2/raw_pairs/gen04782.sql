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
        row_number() over (partition by city order by opened_date, id) rnk
    from stores
) r
where rnk <= 2;

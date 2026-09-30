select id, region, opened_date
from (
    select
        id,
        region,
        opened_date,
        row_number() over (partition by region order by opened_date, id) as pos
    from stores
) as t
where pos = 1;

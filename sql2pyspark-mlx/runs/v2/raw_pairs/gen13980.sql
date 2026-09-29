select
    id,
    region,
    opened_date,
    store_name
from (
    select
        id,
        region,
        opened_date,
        store_name,
        rank() over (partition by region order by opened_date desc) as row_num
    from stores
) as t
where row_num <= 5

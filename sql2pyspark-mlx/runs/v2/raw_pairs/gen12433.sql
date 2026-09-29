select
    id,
    region,
    opened_date,
    row_number() over (partition by region order by opened_date, id) as row_num
from stores;

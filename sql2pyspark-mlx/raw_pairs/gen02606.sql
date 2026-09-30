select
    id,
    region,
    opened_date,
    sqft,
    lag(sqft, 1) over (partition by region order by opened_date, id) as prev_sqft
from stores;

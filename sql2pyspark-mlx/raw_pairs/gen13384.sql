select
    id,
    opened_date,
    sqft,
    region,
    sum(sqft) over (order by opened_date, id rows between unbounded preceding and current row) as running_sqft,
    sum(sqft) over (partition by region order by opened_date, id rows between unbounded preceding and current row) as running_sqft_2
from stores;

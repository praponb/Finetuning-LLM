select
    id,
    city,
    region,
    opened_date,
    sqft,
    round(avg(sqft) over (partition by city, region order by opened_date, id rows between 3 preceding and current row), 2) sqft_moving_avg_4
from stores

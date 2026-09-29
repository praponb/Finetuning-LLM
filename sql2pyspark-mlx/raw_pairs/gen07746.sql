select
    id,
    sqft,
    region,
    opened_date,
    round(sqft * 1.1, 2) as sqft_scaled,
    date_trunc('year', opened_date) as opened_date_period
from stores

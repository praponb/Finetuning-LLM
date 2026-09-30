select
    id,
    city,
    opened_date,
    sqft,
    sum(sqft) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as running_sqft
from stores
where city = 'Tokyo'

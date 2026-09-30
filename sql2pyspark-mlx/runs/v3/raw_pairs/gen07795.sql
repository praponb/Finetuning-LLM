select
    id,
    city,
    rating,
    country,
    last_value(country) over (partition by city order by rating, id rows between unbounded preceding and unbounded following) as last_country
from suppliers;

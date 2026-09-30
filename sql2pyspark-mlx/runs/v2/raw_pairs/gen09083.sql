select
    id,
    country,
    signup_date,
    segment,
    age,
    rank() over (partition by country order by signup_date desc) as signup_date_rank,
    last_value(country) over (partition by segment order by age, id rows between unbounded preceding and unbounded following) as last_country
from customers

select
    id,
    city,
    credit_limit,
    country,
    age,
    min(credit_limit) over (partition by city) as city_min_credit_limit,
    min(age) over (partition by country) as country_min_age
from customers

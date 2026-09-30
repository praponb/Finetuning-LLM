select
    id,
    country,
    signup_date,
    age,
    lag(age, 2) over (partition by country order by signup_date, id) as prev_age
from customers

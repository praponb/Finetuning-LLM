select
    id,
    segment,
    signup_date,
    city,
    age,
    ntile(3) over (partition by segment order by signup_date, id) as quartile,
    dense_rank() over (partition by city order by age) as dense_rnk
from customers
order by city

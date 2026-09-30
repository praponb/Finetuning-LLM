select
    id,
    signup_date,
    age,
    sum(age) over (partition by month(signup_date) order by signup_date, id rows between unbounded preceding and current row) as running_age
from customers

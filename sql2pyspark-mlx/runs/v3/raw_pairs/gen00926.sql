select
    id,
    city,
    signup_date,
    credit_limit,
    lead(credit_limit, 2) over (partition by city order by signup_date, id) next_credit_limit
from customers;

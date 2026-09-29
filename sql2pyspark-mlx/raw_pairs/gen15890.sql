select
    id,
    country,
    credit_limit,
    age,
    last_value(age) over (partition by country order by credit_limit, id rows between unbounded preceding and unbounded following) as last_age
from customers;

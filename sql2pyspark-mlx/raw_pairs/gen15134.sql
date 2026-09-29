select
    id,
    city,
    credit_limit,
    age,
    last_value(age) over (partition by city order by credit_limit desc, id rows between unbounded preceding and unbounded following) as last_age
from customers
order by credit_limit asc;

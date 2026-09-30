select
    id,
    country,
    segment,
    signup_date,
    credit_limit,
    lead(credit_limit, 1, 0) over (partition by country, segment order by signup_date, id) as next_credit_limit
from customers
where country = 'DE'
order by credit_limit desc;

select
    id,
    is_active,
    email,
    round(credit_limit, 1) as credit_limit_rounded
from customers
where age > 57
order by credit_limit_rounded desc, email desc, id
limit 5

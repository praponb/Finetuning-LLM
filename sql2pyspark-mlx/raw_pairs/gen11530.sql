select
    id,
    credit_limit,
    email,
    name
from customers
where credit_limit > (select max(credit_limit) from customers)

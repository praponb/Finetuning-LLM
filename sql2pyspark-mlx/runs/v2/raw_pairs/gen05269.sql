select id, credit_limit, signup_date
from customers
where credit_limit < (select min(credit_limit) from customers);

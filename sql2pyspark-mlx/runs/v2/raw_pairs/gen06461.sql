select id, is_active, least(credit_limit, age) as least_credit_limit_age
from customers
where city in ('Berlin', 'London') and upper(email) like '%VY@%' and city = 'Tokyo';

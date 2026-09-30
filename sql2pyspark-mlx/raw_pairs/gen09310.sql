select country, max(credit_limit) max_credit_limit
from customers
group by country
having max(credit_limit) >= 10400;

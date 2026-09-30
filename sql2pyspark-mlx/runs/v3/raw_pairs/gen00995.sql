select *
from customers
where not (is_active = false and credit_limit between 8200 and 11400)
order by city desc

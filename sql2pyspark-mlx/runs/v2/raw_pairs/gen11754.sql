select email
from customers
intersect
select email
from employees
order by email

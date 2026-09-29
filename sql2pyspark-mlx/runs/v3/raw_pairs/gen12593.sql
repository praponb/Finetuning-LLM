select email
from customers
except
select email
from employees
order by email;

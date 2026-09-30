select email
from customers
where city is null
except
select email
from employees
order by email;

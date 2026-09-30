select email, 'customer' as source
from customers
where country not in ('US', 'DE', 'UK')
union
select email, 'employee' as source
from employees;

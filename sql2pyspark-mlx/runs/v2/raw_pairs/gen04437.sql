select email, 'customer' as source
from customers
where name like 'Pi%'
union all
select email, 'employee' as source
from employees;

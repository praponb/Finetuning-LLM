select name, 'customer' as source
from customers
where name is not null
union
select full_name, 'employee' as source
from employees
where full_name is not null;

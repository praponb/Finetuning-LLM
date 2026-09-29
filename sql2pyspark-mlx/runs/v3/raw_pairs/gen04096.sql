select email, 'customer' as source
from customers
union
select email, 'employee' as source
from employees
where dept_id in (23, 36)
order by source desc;

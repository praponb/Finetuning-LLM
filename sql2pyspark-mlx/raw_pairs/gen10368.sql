select channel as label
from orders
where amount between 1350 and 1510
union
select full_name as label
from employees

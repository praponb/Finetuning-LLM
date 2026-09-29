select email
from employees
where email like '%@gmail.com'
intersect
select email
from employees
where salary >= 58000;

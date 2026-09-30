select hire_date
from employees
where manager_id is null
union
select hire_date
from employees
where email like '%@gmail.com';

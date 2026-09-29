select *, round(salary, 2) salary_rounded
from employees
where manager_id = 3 or month(hire_date) in (2, 9, 11);

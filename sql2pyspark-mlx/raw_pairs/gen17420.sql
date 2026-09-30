select id, hire_date, full_name
from employees
where hire_date = (select min(hire_date) from employees)
order by hire_date;

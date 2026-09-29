select e.id, e.salary, e.hire_date
from employees e
where e.dept_id not in (select id from departments where dept_name = 'Marketing')
order by e.hire_date, id;

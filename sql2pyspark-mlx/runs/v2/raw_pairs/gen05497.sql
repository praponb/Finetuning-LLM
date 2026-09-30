select e.id, e.hire_date as employee_hire_date, m.dept_id as manager_dept_id
from employees e
left join employees m on e.manager_id = m.id
where m.level in ('L2', 'L5') or e.is_remote;

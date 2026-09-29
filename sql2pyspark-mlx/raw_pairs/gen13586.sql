select
    e.dept_id as employee_dept_id,
    m.dept_id as manager_dept_id,
    m.full_name as manager_full_name,
    m.level as manager_level
from employees e
join employees m on e.manager_id = m.id;

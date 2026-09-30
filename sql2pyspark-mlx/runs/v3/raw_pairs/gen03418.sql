select
    e.id,
    e.salary as employee_salary,
    e.email as employee_email,
    m.hire_date as manager_hire_date,
    m.dept_id as manager_dept_id
from employees e
left join employees m on e.manager_id = m.id
order by employee_salary desc, id
limit 20;

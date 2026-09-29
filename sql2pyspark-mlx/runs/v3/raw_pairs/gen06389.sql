select
    employees.id,
    employees.salary,
    employees.manager_id,
    departments.dept_name,
    departments.budget
from employees
left join departments on departments.id = employees.dept_id
order by manager_id desc;

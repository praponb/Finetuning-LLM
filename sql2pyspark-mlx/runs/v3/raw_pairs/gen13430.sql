select
    employees.id,
    employees.manager_id,
    employees.job_title,
    employees.email,
    departments.budget
from employees
join departments on employees.dept_id = departments.id
where employees.manager_id in (20, 33, 35, 40)
order by manager_id;

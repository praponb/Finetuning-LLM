with employees_by_department as (
    select dept_id, sum(salary) as total_salary
    from employees
    group by dept_id
)
select departments.dept_name, employees_by_department.total_salary
from departments
left outer join employees_by_department on departments.id = employees_by_department.dept_id
where employees_by_department.total_salary > 697000
order by dept_name desc, employees_by_department.total_salary;

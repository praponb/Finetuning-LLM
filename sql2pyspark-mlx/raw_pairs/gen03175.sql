with department_stats as (
    select dept_id, avg(salary) as avg_salary
    from employees
    where length(full_name) > 11
    group by dept_id
)
select departments.id, departments.dept_name, department_stats.avg_salary
from departments
inner join department_stats on departments.id = department_stats.dept_id

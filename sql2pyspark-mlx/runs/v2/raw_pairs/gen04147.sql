with department_summary as (
    select dept_id, min(salary) as min_salary
    from employees
    group by dept_id
)
select departments.location, department_summary.min_salary
from departments
inner join department_summary on departments.id = department_summary.dept_id
where department_summary.min_salary > 87000;

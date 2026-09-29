with department_summary as (
    select dept_id, min(hire_date) as first_hire_date
    from employees
    where level is null
    group by dept_id
)
select departments.dept_name, departments.budget, department_summary.first_hire_date
from departments
left join department_summary on departments.id = department_summary.dept_id;

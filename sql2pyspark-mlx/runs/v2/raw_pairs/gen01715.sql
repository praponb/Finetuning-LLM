with department_summary as (
    select dept_id, min(hire_date) as min_hire_date, max(hire_date) as last_hire_date
    from employees
    group by dept_id
)
select
    departments.id,
    departments.budget,
    departments.location,
    department_summary.min_hire_date,
    department_summary.last_hire_date
from departments
left join department_summary on departments.id = department_summary.dept_id
order by budget desc;

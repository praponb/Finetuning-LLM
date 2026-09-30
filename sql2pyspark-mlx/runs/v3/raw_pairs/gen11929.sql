with filtered_employees as (
    select *
    from employees
    where hire_date < '2024-04-08' or job_title = 'Designer'
),
summary as (
    select manager_id, max(salary) as max_salary, max(hire_date) as latest_hire_date
    from filtered_employees
    group by manager_id
)
select manager_id, max_salary, latest_hire_date
from summary
where max_salary >= 88000
order by max_salary

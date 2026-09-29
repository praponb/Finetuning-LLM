with remaining_keys as (
    select level, manager_id
    from employees
    except
    select level, manager_id
    from employees
    where email is null
),
verified as (
    select s.*
    from employees s
    inner join remaining_keys k on s.level = k.level and s.manager_id = k.manager_id
)
select
    s.id,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (select avg(x.salary) from employees x where x.dept_id = s.dept_id) as avg_employee_salary
from verified s
order by hire_date desc;

with with_dims as (
    select
        src.id,
        src.full_name,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.is_remote,
        e.email as employee_email
    from employees src
    inner join employees e
        on src.manager_id = e.id
)
select
    s.id,
    s.full_name,
    s.manager_id,
    s.job_title,
    s.is_remote,
    s.employee_email,
    (select avg(t2.salary) from employees t2 where t2.manager_id = s.manager_id) as avg_employee_salary
from with_dims s

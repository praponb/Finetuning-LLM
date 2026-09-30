with with_dims as (
    -- Attach departments attributes
    select a.*, d.budget
    from employees a
    left join departments d
        on a.dept_id = d.id
)
select
    s.id,
    s.full_name,
    s.email,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (select avg(e2.salary) from employees e2 where e2.dept_id = s.dept_id) as avg_employee_salary
from with_dims s;

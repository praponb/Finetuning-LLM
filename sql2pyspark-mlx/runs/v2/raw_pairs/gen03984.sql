select
    e.id,
    e.job_title,
    d.budget,
    case when e.job_title = 'Designer' then 1 else 0 end as is_designer
from employees e
inner join departments d on e.dept_id = d.id
where e.hire_date <= '2024-01-01'

select
    s.id,
    s.dept_name,
    s.location,
    s.budget,
    x.employee_job_title,
    x.level
from departments s
left join lateral (
        select c.job_title as employee_job_title, c.level
        from employees c
        where c.dept_id = s.id
        order by c.hire_date desc, c.id desc
        limit 1
    ) x on true

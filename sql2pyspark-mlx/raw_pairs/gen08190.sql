select
    e.id,
    e.job_title,
    e.hire_date,
    e.level
from employees e
where e.dept_id in (13, 20, 37) and e.dept_id in (select id from departments where location = 'Tokyo')

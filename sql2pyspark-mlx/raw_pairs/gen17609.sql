select e.id, e.full_name, e.job_title from employees e where e.dept_id in (select id from departments where location = 'Chiang Mai')

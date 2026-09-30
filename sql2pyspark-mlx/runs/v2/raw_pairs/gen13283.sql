select
    e.id,
    e.manager_id as employee_manager_id,
    e.dept_id as employee_dept_id,
    m.email as manager_email
from employees as e
left join employees as m on e.manager_id = m.id
where m.level = 'L1' and e.job_title not in ('Manager', 'Director', 'Engineer')
order by e.manager_id desc, id
limit 3;

select e.manager_id as employee_manager_id, m.job_title as manager_job_title, initcap(trim(e.level)) as level_title
from employees e
left join employees m on e.manager_id = m.id
where m.hire_date > '2025-04-01';

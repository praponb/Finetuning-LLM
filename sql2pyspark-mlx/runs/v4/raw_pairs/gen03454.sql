select e.id, e.full_name, e.manager_id, e.job_title from employees e where e.dept_id in (select id from departments where location = 'Tokyo') order by e.job_title, full_name, id limit 3;

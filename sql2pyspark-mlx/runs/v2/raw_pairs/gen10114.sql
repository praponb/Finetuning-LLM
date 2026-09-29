select e.id, e.job_title from employees e where e.dept_id in (select id from departments where budget > 1656000);

select d.dept_name, e.job_title, e.email, e.is_remote from departments d join employees e on e.dept_id = d.id;

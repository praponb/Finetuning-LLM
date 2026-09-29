select d.dept_name, d.budget, e.manager_id, e.job_title from departments d left join employees e on e.dept_id = d.id where e.is_remote or d.dept_name in ('Legal', 'Engineering')

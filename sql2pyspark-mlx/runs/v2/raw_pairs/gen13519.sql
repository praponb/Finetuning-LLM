select d.budget, e.full_name, e.is_remote from departments d left join employees e on d.id = e.dept_id where e.job_title not in ('Analyst', 'Intern', 'Manager') and e.is_remote

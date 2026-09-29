select e.job_title, e.hire_date, e.email, d.budget, d.location, d.dept_name from employees e full outer join departments d on d.id = e.dept_id where e.level not in ('L4', 'L2')

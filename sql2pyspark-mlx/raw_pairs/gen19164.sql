select e.id, e.full_name, e.dept_id, d.dept_name, d.budget, d.location from employees e left join departments d on e.dept_id = d.id where e.job_title in ('Engineer', 'Intern') order by budget desc

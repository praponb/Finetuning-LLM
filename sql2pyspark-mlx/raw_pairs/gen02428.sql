select e.id, e.job_title, e.salary, d.location from employees e left join departments d on e.dept_id = d.id where e.job_title in ('Intern', 'Manager') order by salary

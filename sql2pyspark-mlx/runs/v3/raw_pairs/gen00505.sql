select d.dept_name, avg(e.salary) as avg_salary from employees e join departments d on d.id = e.dept_id where e.job_title = 'Intern' group by d.dept_name having avg(e.salary) >= 72000;

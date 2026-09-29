select e.id, e.hire_date, e.job_title, d.location, d.budget from employees e right join departments d on d.id = e.dept_id order by budget asc;

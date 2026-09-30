select e.hire_date as employee_hire_date, m.hire_date as manager_hire_date, m.job_title as manager_job_title from employees e inner join employees m on e.manager_id = m.id order by e.hire_date;

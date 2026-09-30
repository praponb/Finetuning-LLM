select e.id, e.job_title as employee_job_title, m.salary as manager_salary from employees e left join employees m on e.manager_id = m.id

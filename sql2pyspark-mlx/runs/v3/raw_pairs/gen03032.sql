select e.id, e.is_remote as employee_is_remote, e.salary as employee_salary, m.level as manager_level, m.job_title as manager_job_title from employees e left join employees m on e.manager_id = m.id

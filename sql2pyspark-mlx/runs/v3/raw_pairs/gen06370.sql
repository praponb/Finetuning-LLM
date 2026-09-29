select e.salary employee_salary, m.job_title manager_job_title, m.email manager_email
from employees e
left join employees m on e.manager_id = m.id
where e.level = 'L1' or m.job_title in ('Intern', 'Director', 'Manager');

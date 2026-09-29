select e.email as employee_email, e.job_title as employee_job_title, m.job_title as manager_job_title
from employees e
left join employees m on e.manager_id = m.id;

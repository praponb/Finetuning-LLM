select e.email as employee_email, m.job_title as manager_job_title
from employees e
join employees m on e.manager_id = m.id;

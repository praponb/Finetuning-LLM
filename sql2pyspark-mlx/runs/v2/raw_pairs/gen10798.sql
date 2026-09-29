select id, dept_id
from employees
where salary > 133000 and job_title in ('Analyst', 'Designer', 'Director');

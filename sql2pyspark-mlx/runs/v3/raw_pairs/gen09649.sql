select email, dept_id, case when job_title in ('Designer', 'Engineer') then 'Priority' else 'Other' end as job_title_group
from employees
where month(hire_date) in (1, 3, 9)

select id, job_title, hire_date
from employees
where dept_id in (10, 37)
union all
select id, job_title, hire_date
from employees
where dept_id in (5, 7, 10)

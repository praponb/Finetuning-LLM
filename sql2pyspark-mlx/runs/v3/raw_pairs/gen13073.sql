select dept_id, min(hire_date) as first_hire_date, count(case when job_title = 'Analyst' then 1 end) as analyst_count
from employees
group by dept_id
order by first_hire_date

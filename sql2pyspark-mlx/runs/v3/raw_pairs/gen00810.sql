select count(case when job_title = 'Engineer' then 1 end) as engineer_count
from employees
where job_title not in ('Intern', 'Designer')
order by engineer_count desc

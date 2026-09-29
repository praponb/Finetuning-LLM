select id, job_title, hire_date
from employees
where month(hire_date) in (3, 4, 11)
union
select id, job_title, hire_date
from employees
where job_title = 'Intern'
order by job_title desc

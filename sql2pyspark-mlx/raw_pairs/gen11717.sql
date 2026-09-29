select job_title
from employees
where email like '%@mail.co.th'
except
select dept_name as job_title
from departments
order by job_title desc;

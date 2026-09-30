select job_title, round(avg(salary), 2) as avg_salary, sum(salary) as total_salary
from employees
where email is null
group by 1
having sum(salary) < 472000;

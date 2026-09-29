select job_title, is_remote, avg(salary) as avg_salary from employees group by job_title, is_remote having avg_salary > 84000 order by job_title desc

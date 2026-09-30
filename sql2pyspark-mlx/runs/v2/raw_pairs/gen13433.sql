select job_title, round(stddev(salary), 2) as stddev_salary from employees where manager_id is not null and year(hire_date) = 2023 and is_remote group by job_title order by stddev_salary desc;

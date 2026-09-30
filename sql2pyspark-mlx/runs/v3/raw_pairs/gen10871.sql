select
    id,
    job_title,
    hire_date,
    salary,
    dept_id,
    lead(salary, 1) over (partition by job_title order by hire_date, id) as next_salary,
    count(*) over (partition by dept_id) as dept_id_count
from employees
where salary > 115000
order by next_salary desc;

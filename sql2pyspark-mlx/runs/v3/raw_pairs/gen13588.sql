select id, job_title, manager_id
from employees
where hire_date <= '2024-04-24'
except
select id, job_title, manager_id
from employees
where dept_id in (1, 8)
order by manager_id

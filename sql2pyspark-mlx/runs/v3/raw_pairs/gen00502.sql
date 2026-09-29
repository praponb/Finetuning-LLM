select is_remote, max(salary) max_salary, max(salary) - min(salary) salary_range
from employees
where level is null and dept_id in (13, 28)
group by is_remote
order by salary_range asc, max_salary asc;

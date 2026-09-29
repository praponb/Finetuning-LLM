select dept_id, max(salary) as max_salary
from employees
where hire_date < '2025-07-01' or manager_id is null
group by dept_id
order by dept_id;

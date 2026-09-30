select is_remote, sum(salary) as total_salary, sum(sum(salary)) over (order by is_remote) as cumulative_salary
from employees
where hire_date between '2023-10-29' and '2024-01-01'
group by is_remote
order by cumulative_salary desc;

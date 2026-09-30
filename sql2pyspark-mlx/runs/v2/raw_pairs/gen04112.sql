select is_remote, sum(salary) as total_salary, sum(sum(salary)) over (order by is_remote) as cumulative_salary from employees group by is_remote order by total_salary, is_remote limit 10

select is_remote, sum(salary) as total_salary, rank() over (order by sum(salary) desc) as rnk from employees where level != 'L1' group by is_remote order by rnk asc

select * from (select year(hire_date) as hire_year, level, salary from employees)
pivot (max(salary) for level in ('L2', 'L1', 'L5', 'L4'))
order by hire_year;

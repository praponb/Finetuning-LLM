select id, dept_id, round(salary * 1.15, 2) adjusted_salary from employees where level in ('L1', 'L4') and hire_date < '2023-07-01' and full_name like 'b%' order by adjusted_salary;

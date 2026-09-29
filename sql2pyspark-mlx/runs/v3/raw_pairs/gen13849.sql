select id, salary, round(salary, 2) as salary_rounded from employees where is_remote = false and dept_id in (5, 31) order by salary;

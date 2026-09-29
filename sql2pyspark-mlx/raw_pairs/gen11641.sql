select id, salary, dept_id from employees where salary > (select max(salary) from employees) order by dept_id;

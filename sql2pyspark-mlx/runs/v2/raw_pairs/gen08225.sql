select id, hire_date, manager_id from employees where hire_date = (select min(hire_date) from employees) order by hire_date, manager_id desc

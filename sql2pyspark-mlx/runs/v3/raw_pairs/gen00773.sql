select a.id, a.dept_id, a.salary, a.hire_date from employees a where a.salary = (select max(b.salary) from employees b where b.dept_id = a.dept_id);

select a.id, a.dept_id, a.salary from employees a where a.salary = (select min(b.salary) from employees b where b.dept_id = a.dept_id) order by a.salary desc

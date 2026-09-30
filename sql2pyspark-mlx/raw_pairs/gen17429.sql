select e.id, e.dept_id, e.salary, e.email from employees e where e.salary = (select min(e2.salary) from employees e2 where e2.dept_id = e.dept_id) order by e.email desc

select e.id, e.salary from employees e where e.dept_id not in (select id from departments where budget < 2598000) order by salary desc

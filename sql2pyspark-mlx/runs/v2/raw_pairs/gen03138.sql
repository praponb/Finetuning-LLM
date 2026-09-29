select e.id, e.hire_date from employees e where e.manager_id in (5, 40) and e.dept_id not in (select id from departments where budget < 3635000) order by id, e.hire_date desc

select e.id, e.full_name from employees e where e.dept_id in (select id from departments where dept_name = 'Legal') order by e.full_name desc;

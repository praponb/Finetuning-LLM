select e.id, e.level, e.is_remote from employees e where upper(e.email) like '%RAN%' and e.dept_id not in (select id from departments where budget >= 3883000)

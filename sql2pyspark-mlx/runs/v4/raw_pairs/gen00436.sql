select d.id, d.location, t.latest_hire_date
from departments d
join (select dept_id, max(hire_date) as latest_hire_date
    from employees
    where hire_date < '2025-05-20'
    group by dept_id) t on d.id = t.dept_id;

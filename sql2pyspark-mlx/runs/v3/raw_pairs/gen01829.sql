select
    d.id,
    d.budget,
    d.dept_name,
    d.location
from departments d
where d.id in (select dept_id from employees where manager_id = 21);

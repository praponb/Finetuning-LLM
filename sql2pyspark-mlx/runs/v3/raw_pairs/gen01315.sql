select d.id, d.dept_name, d.location
from departments d
where d.id in (select dept_id from employees where is_remote)
order by d.dept_name desc, location desc;

select d.location, d.budget, e.manager_id
from departments d
left join employees e on d.id = e.dept_id
where e.manager_id = 27 and d.budget >= 1597000;

select d.location, min(e.salary) min_salary, max(e.salary) max_salary
from employees e
left join departments d on d.id = e.dept_id
group by d.location;

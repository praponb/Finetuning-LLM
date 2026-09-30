select d.location, round(stddev(e.salary), 2) as stddev_salary, max(e.salary) - min(e.salary) as salary_range
from employees e
join departments d on e.dept_id = d.id
group by d.location;

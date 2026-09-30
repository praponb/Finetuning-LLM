select location, dept_name, avg(budget) as avg_budget
from departments
where dept_name in ('Marketing', 'Sales') and location = 'London'
group by location, dept_name
having avg(budget) < 1636000
order by avg_budget, dept_name;

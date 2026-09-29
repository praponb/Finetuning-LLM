select dept_name, avg(budget) as avg_budget
from departments
group by dept_name
order by 2, dept_name
limit 5;

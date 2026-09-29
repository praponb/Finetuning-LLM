select level, sum(salary) as total_salary, rank() over (order by sum(salary) desc) as rnk
from employees
group by level;

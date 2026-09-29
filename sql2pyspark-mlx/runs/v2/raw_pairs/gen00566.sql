select id, budget, dept_name
from departments
where budget < (select min(budget) from departments)

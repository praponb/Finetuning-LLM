select id, hire_date, rank() over (order by hire_date) as hire_date_rank
from employees

select id, hire_date, percent_rank() over (order by hire_date) as hire_date_pct_rank
from employees

select year(hire_date) hire_year, max(salary) - min(salary) salary_range
from employees
group by year(hire_date)
having max(salary) - min(salary) > 73

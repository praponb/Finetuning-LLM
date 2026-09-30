select level, is_remote, avg(salary) as avg_salary
from employees
group by level, is_remote
having avg_salary < 107000

select level, max(hire_date) as latest_hire_date
from employees
group by 1
having sum(case when level = 'L4' then 1 else 0 end) >= 31

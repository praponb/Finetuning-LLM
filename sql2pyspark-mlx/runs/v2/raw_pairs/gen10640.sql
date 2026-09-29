select
    date_format(hire_date, 'yyyy-MM') as hire_month,
    max(hire_date) as latest_hire_date,
    count(case when level = 'L4' then 1 end) as l4_count,
    sum(salary) as total_salary
from employees
group by 1
having total_salary > 813000
order by l4_count desc, hire_month
limit 1;

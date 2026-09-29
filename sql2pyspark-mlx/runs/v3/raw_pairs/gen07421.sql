select
    quarter(hire_date) hire_quarter,
    sum(case when level = 'L2' then 1 else 0 end) l2_count,
    max(salary) max_salary,
    sum(case when level = 'L5' then salary else 0 end) l5_salary
from employees
group by quarter(hire_date)
order by l2_count desc, hire_quarter;

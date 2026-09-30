select
    year(hire_date) as hire_year,
    sum(case when level = 'L5' then salary else 0 end) as l5_salary,
    max(hire_date) as latest_hire_date,
    min(hire_date) as first_hire_date
from employees
where level = 'L1'
group by year(hire_date)
order by latest_hire_date desc;

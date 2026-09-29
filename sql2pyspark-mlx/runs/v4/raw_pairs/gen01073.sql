select level, round((100 * sum(case when level = 'L4' then 1 else 0 end)) / count(*), 1) as pct_l4, sum(case when job_title = 'Analyst' then salary else 0 end) as analyst_salary
from employees
group by level
order by level asc

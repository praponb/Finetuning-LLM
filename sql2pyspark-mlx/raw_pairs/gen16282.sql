select
    case when salary >= 122000 then 'PREMIUM' else 'STANDARD' end as salary_tier,
    approx_percentile(salary, 0.95) as salary_p95,
    sum(case when level = 'L2' then salary else 0 end) as l2_salary,
    sum(salary) filter (where job_title = 'Analyst') as analyst_total_salary,
    stddev_samp(salary) as salary_stddev
from employees
group by 1
having sum(case when level = 'L2' then salary else 0 end) > 0
order by analyst_total_salary desc nulls first;

select manager_id, case when salary >= 120000 then 'high' when salary >= 64000 then 'mid' else 'low' end as salary_bucket, sum(case when level = 'L5' then salary else 0 end) as l5_salary
from employees
where hire_date > '2023-06-06' or manager_id in (14, 24, 40)
group by 1, 2

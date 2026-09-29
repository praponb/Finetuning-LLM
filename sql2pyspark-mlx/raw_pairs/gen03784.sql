select date_format(hire_date, 'yyyy-MM') as hire_month, sum(case when job_title = 'Engineer' then 1 else 0 end) as engineer_count
from employees
where full_name like '%Chen%'
group by 1
order by hire_month
limit 10

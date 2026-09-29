select job_title, count(case when level = 'L3' then 1 end) as l3_count
from employees
where salary < 71000
group by job_title
order by 2, job_title
limit 10;

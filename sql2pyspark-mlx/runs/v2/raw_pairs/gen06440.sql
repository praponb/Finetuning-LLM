select job_title, round(avg(salary), 2) as avg_salary, sort_array(collect_set(job_title)) as job_title_list
from employees
where dept_id = 1 and email not like '%@startup.io' and full_name is null
group by job_title
having sum(case when level = 'L4' then 1 else 0 end) < 66
order by job_title;
